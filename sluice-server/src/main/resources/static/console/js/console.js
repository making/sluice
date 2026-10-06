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
