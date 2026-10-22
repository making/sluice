// Mirrors the outcome of each live poll onto <html data-link>, so a view that stopped
// refreshing (server down, network error, 5xx) is marked stale instead of looking current.
document.addEventListener("htmx:finally:request", (event) => {
  if (!(event.target instanceof Element) || !event.target.matches("[data-poller]")) {
    return;
  }
  const status = event.detail.ctx?.response?.status;
  document.documentElement.dataset.link = status !== undefined && status < 400 ? "live" : "lost";
});

// On narrow screens the drawing scrolls inside its frame; open it centred on the lock.
{
  const sheet = document.querySelector(".sheet__drawing");
  if (sheet && sheet.scrollWidth > sheet.clientWidth) {
    sheet.scrollLeft = (sheet.scrollWidth - sheet.clientWidth) / 2;
  }
}

// The live access log: rows rendered by the server arrive over SSE, newest last. The
// event id is the flume's position, so the browser's own reconnect resumes where it
// stopped; pausing closes the stream and resuming reopens it after the last row.
{
  const rows = document.getElementById("livelog-rows");
  const form = document.querySelector("[data-livelog-filters]");
  if (rows && form) {
    const state = document.querySelector("[data-testid=livelog-state]");
    const missed = document.querySelector("[data-testid=livelog-missed]");
    const nodes = document.getElementById("livelog-nodes");
    const build = document.getElementById("livelog-build");
    const pause = document.querySelector("[data-livelog-pause]");
    const follow = document.querySelector("[data-livelog-follow]");
    const scroll = document.querySelector("[data-livelog-scroll]");
    const cap = Number(rows.dataset.rowCap) || 1000;
    const labels = {
      connecting: "Connecting",
      up: "Live",
      down: "Flume unreachable",
      paused: "Paused",
      closed: "Disconnected: reload the page",
    };
    let source = null;
    let lastId = "";
    let dropped = 0;

    const setState = (value) => {
      state.dataset.state = value;
      state.textContent = labels[value] ?? value;
    };
    const query = () => {
      const params = new URLSearchParams();
      for (const [name, value] of new FormData(form)) {
        if (value !== "") {
          params.append(name, String(value));
        }
      }
      return params;
    };
    const pad = (value, width = 2) => String(value).padStart(width, "0");
    // the server sends the instant; the time column reads it in the browser's zone
    const localTime = (time) => {
      const at = new Date(time.dateTime);
      if (!Number.isNaN(at.getTime())) {
        time.textContent = `${pad(at.getHours())}:${pad(at.getMinutes())}:${pad(at.getSeconds())}.${pad(at.getMilliseconds(), 3)}`;
        time.title = at.toString();
      }
    };
    const append = (html) => {
      const template = document.createElement("template");
      template.innerHTML = html.trim();
      const row = template.content.firstElementChild;
      if (!row) {
        return null;
      }
      row.querySelectorAll("time[datetime]").forEach(localTime);
      rows.append(row);
      while (rows.rows.length > cap) {
        rows.deleteRow(0);
      }
      if (follow.checked) {
        scroll.scrollTop = scroll.scrollHeight;
      }
      return row;
    };
    const open = () => {
      const params = query();
      if (lastId) {
        params.set("after", lastId);
      }
      source = new EventSource(rows.dataset.livelogStream + "?" + params);
      setState("connecting");
      source.addEventListener("row", (event) => {
        lastId = event.lastEventId || lastId;
        append(event.data);
      });
      source.addEventListener("gap", (event) => {
        lastId = event.lastEventId || lastId;
        dropped += Number(append(event.data)?.dataset.dropped ?? 0);
        missed.textContent = String(dropped);
      });
      source.addEventListener("nodes", (event) => {
        nodes.innerHTML = event.data;
      });
      source.addEventListener("flume", (event) => setState(event.data));
      source.addEventListener("build", (event) => {
        build.innerHTML = event.data;
      });
      // the browser retries on its own unless the answer was no stream at all (the
      // session expired, say)
      source.addEventListener("error", (event) => {
        if (event.target.readyState === EventSource.CLOSED) {
          setState("closed");
        }
        else if (state.dataset.state !== "down") {
          setState("connecting");
        }
      });
    };
    const close = () => {
      source?.close();
      source = null;
    };

    form.addEventListener("change", () => {
      close();
      lastId = "";
      dropped = 0;
      missed.textContent = "0";
      rows.replaceChildren();
      history.replaceState(null, "", "?" + query());
      if (pause.getAttribute("aria-pressed") !== "true") {
        open();
      }
    });
    pause.addEventListener("click", () => {
      const paused = pause.getAttribute("aria-pressed") !== "true";
      pause.setAttribute("aria-pressed", String(paused));
      pause.textContent = paused ? "Resume" : "Pause";
      if (paused) {
        close();
        setState("paused");
      }
      else {
        open();
      }
    });
    // scrolling up to read stops the follow; back at the bottom it resumes
    scroll.addEventListener("scroll", () => {
      follow.checked = scroll.scrollHeight - scroll.scrollTop - scroll.clientHeight < 8;
    });
    open();
  }
}
