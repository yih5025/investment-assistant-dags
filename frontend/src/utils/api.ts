// Resolve API base URL with HTTPS upgrade to avoid mixed-content in HTTPS pages
const rawApiBase: string = (import.meta as any)?.env?.VITE_API_BASE ?? "/api";

function upgradeToHttpsIfNeeded(base: string): string {
  try {
    if (typeof window !== "undefined" && window.location?.protocol === "https:" && base.startsWith("http://")) {
      return base.replace(/^http:\/\//, "https://");
    }
  } catch {}
  return base;
}

export const API_BASE: string = upgradeToHttpsIfNeeded(rawApiBase);

export function apiUrl(path: string): string {
  if (!path) return API_BASE;
  const hasBaseSlash = API_BASE.endsWith("/");
  const hasPathSlash = path.startsWith("/");
  if (hasBaseSlash && hasPathSlash) return API_BASE + path.slice(1);
  if (!hasBaseSlash && !hasPathSlash) return `${API_BASE}/${path}`;
  return API_BASE + path;
}


