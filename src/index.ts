// Janitor AI -> Anthropic bridge (Cloudflare Worker)
// Secrets needed: ANTHROPIC_API_KEY (required), PROXY_PASSWORD (optional but recommended)

const ANTHROPIC_URL = "https://api.anthropic.com/v1/messages";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key, anthropic-version",
"Access-Control-Max-Age": "86400",
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...CORS, "Content-Type": "application/json" },
  });
}

function textOf(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map((p) => (typeof p === "string" ? p : p.text || "")).join("");
  }
  return "";
}

function pushMsg(msgs, role, text) {
  const prev = msgs[msgs.length - 1];
  if (prev && prev.role === role) prev.content += "\n\n" + text;
  else msgs.push({ role, content: text });
}

// OpenAI messages -> Anthropic system + alternating user/assistant messages
function convertMessages(openaiMessages) {
  const systemParts = [];
  const msgs = [];
  for (const m of openaiMessages) {
    const text = textOf(m.content);
    if (!text) continue;
    if (m.role === "system") {
      if (msgs.length === 0) systemParts.push(text);
      else pushMsg(msgs, "user", text);
    } else {
      pushMsg(msgs, m.role === "assistant" ? "assistant" : "user", text);
    }
  }
  if (msgs.length === 0 || msgs[0].role !== "user") {
    msgs.unshift({ role: "user", content: "(Begin the roleplay.)" });
  }
  if (msgs[msgs.length - 1].role !== "user") {
    msgs.push({ role: "user", content: "(Continue.)" });
  }
  return { system: systemParts.join("\n\n"), msgs };
}

// Anthropic SSE stream -> OpenAI SSE stream
function convertStream(upstreamBody, model) {
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const id = "chatcmpl-" + Date.now();
  const created = Math.floor(Date.now() / 1000);
  let buf = "";

  const chunk = (delta, finish = null) =>
    enc.encode(
      "data: " +
        JSON.stringify({
          id,
          object: "chat.completion.chunk",
          created,
          model,
          choices: [{ index: 0, delta, finish_reason: finish }],
        }) +
        "\n\n"
    );

  const ts = new TransformStream({
    start(controller) {
      controller.enqueue(chunk({ role: "assistant", content: "" }));
    },
    transform(raw, controller) {
      buf += dec.decode(raw, { stream: true });
      const lines = buf.split("\n");
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith("data:")) continue;
        const d = line.slice(5).trim();
        if (!d) continue;
        let ev;
        try {
          ev = JSON.parse(d);
        } catch {
          continue;
        }
        if (ev.type === "content_block_delta" && ev.delta && ev.delta.type === "text_delta") {
          controller.enqueue(chunk({ content: ev.delta.text }));
        } else if (ev.type === "message_delta") {
          const reason = ev.delta && ev.delta.stop_reason === "max_tokens" ? "length" : "stop";
          controller.enqueue(chunk({}, reason));
        } else if (ev.type === "error") {
          const msg = (ev.error && ev.error.message) || "unknown error";
          controller.enqueue(chunk({ content: "\n[Anthropic error: " + msg + "]" }, "stop"));
        }
      }
    },
    flush(controller) {
      controller.enqueue(enc.encode("data: [DONE]\n\n"));
    },
  });

  return upstreamBody.pipeThrough(ts);
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: CORS });
    }

    const url = new URL(request.url);
    if (!url.pathname.endsWith("/chat/completions")) {
      return new Response("Claude bridge active", { status: 200, headers: CORS });
    }

    // Optional password so strangers can't use your key
    if (env.PROXY_PASSWORD) {
      const auth = request.headers.get("Authorization") || "";
      if (auth !== "Bearer " + env.PROXY_PASSWORD) {
        return json({ error: { message: "Unauthorized: wrong proxy password" } }, 401);
      }
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return json({ error: { message: "Invalid JSON body" } }, 400);
    }

    const { system, msgs } = convertMessages(body.messages || []);

    // Prompt caching: mark the system prompt and the latest message
    const last = msgs[msgs.length - 1];
    last.content = [{ type: "text", text: last.content, cache_control: { type: "ephemeral" } }];

    const payload = {
      model: body.model,
      max_tokens: body.max_tokens || 4096,
      stream: !!body.stream,
      messages: msgs,
    };
    if (system) {
      payload.system = [{ type: "text", text: system, cache_control: { type: "ephemeral" } }];
    }
    if (typeof body.temperature === "number") {
      payload.temperature = Math.min(Math.max(body.temperature, 0), 1);
    }

    let upstream;
    try {
      upstream = await fetch(ANTHROPIC_URL, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": env.ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify(payload),
      });
    } catch (err) {
      return json({ error: { message: "Could not reach Anthropic: " + err.message } }, 502);
    }

    if (!upstream.ok) {
      const t = await upstream.text();
      return json({ error: { message: "Anthropic " + upstream.status + ": " + t } }, upstream.status);
    }

    if (body.stream) {
      return new Response(convertStream(upstream.body, body.model), {
        headers: {
          ...CORS,
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
        },
      });
    }

    const data = await upstream.json();
    const text = (data.content || [])
      .filter((b) => b.type === "text")
      .map((b) => b.text)
      .join("");
    return json({
      id: data.id,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model: data.model,
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: text },
          finish_reason: data.stop_reason === "max_tokens" ? "length" : "stop",
        },
      ],
      usage: {
        prompt_tokens: (data.usage && data.usage.input_tokens) || 0,
        completion_tokens: (data.usage && data.usage.output_tokens) || 0,
      },
    });
  },
};
