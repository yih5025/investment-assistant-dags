export const API_BASE: string = (import.meta as any)?.env?.VITE_API_BASE || "/api/v1";

export function apiUrl(path: string): string {
  if (!path) return API_BASE;
  const hasBaseSlash = API_BASE.endsWith("/");
  const hasPathSlash = path.startsWith("/");
  if (hasBaseSlash && hasPathSlash) return API_BASE + path.slice(1);
  if (!hasBaseSlash && !hasPathSlash) return `${API_BASE}/${path}`;
  return API_BASE + path;
}


