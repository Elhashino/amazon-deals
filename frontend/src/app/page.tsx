import { fetchDeals } from "../lib/api";

function pct(x: number | null) {
  if (x === null || x === undefined) return "-";
  return `${Math.round(x * 100)}%`;
}

export default async function Page({
  searchParams,
}: {
  searchParams?: { sort?: string };
}) {
  const sort = searchParams?.sort === "deal" ? "deal" : "hot";
  const data = await fetchDeals(undefined, sort);
  const items = data.items || [];

  return (
    <main>
      <h2>Top deals (all categories)</h2>
      <p style={{ opacity: 0.8 }}>
        {sort === "hot"
          ? "Sorted by Hot Score (deal quality + demand)."
          : "Sorted by Deal Score (discount strength + price stability)."}
      </p>

      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 12 }}>
        <span style={{ opacity: 0.8 }}>Sort:</span>
        <a href="/?sort=hot" style={{ fontWeight: sort === "hot" ? 700 : 400 }}>
          Most wanted
        </a>
        <a href="/?sort=deal" style={{ fontWeight: sort === "deal" ? 700 : 400 }}>
          Best deal
        </a>
      </div>

      <div style={{ display: "grid", gap: 12 }}>
        {items.map((d: any) => (
          <div key={`${d.asin}-${d.category}`} style={{ border: "1px solid #ddd", borderRadius: 10, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 12, opacity: 0.8 }}>
                  {String(d.category).toUpperCase()} • Hot {Math.round(d.hot_score ?? d.score ?? 0)} • Deal{" "}
                  {Math.round(d.score ?? 0)}
                  {d.demand_score !== null && d.demand_score !== undefined
                    ? ` • Demand ${Math.round(d.demand_score ?? 0)}`
                    : ""}
                </div>
                <a href={`/deal/${d.asin}`} style={{ fontWeight: 700 }}>
                  {d.title}
                </a>
                <div style={{ fontSize: 13, marginTop: 6 }}>
                  Now: <b>£{(d.price_current ?? 0).toFixed(2)}</b> • Typical (90d median): £
                  {(d.price_median_90d ?? 0).toFixed(2)} • Drop: <b>{pct(d.discount_pct_90d)}</b> • Confidence:{" "}
                  {Math.round(d.confidence ?? 0)}
                </div>
              </div>
              <div style={{ whiteSpace: "nowrap" }}>
                <a href={d.amazon_url} target="_blank">
                  View on Amazon
                </a>
              </div>
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
