export const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

export async function fetchDeals(category?: string, sort: "hot" | "deal" = "hot") {
  const url = new URL(`${API_BASE}/api/deals`);
  url.searchParams.set("limit", "100");
  url.searchParams.set("sort", sort);
  if (category) url.searchParams.set("category", category);
  const res = await fetch(url.toString(), { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load deals");
  return res.json();
}

export async function fetchDeal(asin: string) {
  const res = await fetch(`${API_BASE}/api/deal/${asin}`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load deal");
  return res.json();
}
