import { fetchDeal } from "../../../lib/api";

export default async function DealPage({ params }: { params: { asin: string } }) {
  const d = await fetchDeal(params.asin);

  return (
    <main>
      <h2>{d.title}</h2>
      <div style={{ opacity: 0.85 }}>{d.brand ? `Brand: ${d.brand}` : ""}</div>

      <div style={{ marginTop: 12, border: "1px solid #ddd", borderRadius: 10, padding: 12 }}>
        <div>
          Category: <b>{d.category}</b>
        </div>
        <div>
          Now: <b>£{(d.price_current ?? 0).toFixed(2)}</b>
        </div>
        <div>Typical (90d median): £{(d.price_median_90d ?? 0).toFixed(2)}</div>
        <div>
          Drop vs typical: <b>{Math.round((d.discount_pct_90d ?? 0) * 100)}%</b>
        </div>
        <div>Confidence: {Math.round(d.confidence ?? 0)}</div>
        <div>Deal Score: {Math.round(d.score ?? 0)}</div>
        {d.hot_score !== null && d.hot_score !== undefined ? <div>Hot Score: {Math.round(d.hot_score)}</div> : null}
        {d.demand_score !== null && d.demand_score !== undefined ? (
          <div style={{ opacity: 0.85 }}>
            Demand: {Math.round(d.demand_score)}
            {d.sales_rank_current ? ` • Rank ${Math.round(d.sales_rank_current)}` : ""}
            {d.rating ? ` • Rating ${Number(d.rating).toFixed(1)}` : ""}
            {d.review_count ? ` • Reviews ${d.review_count}` : ""}
          </div>
        ) : null}
        <div style={{ marginTop: 10 }}>
          <a href={d.amazon_url} target="_blank">
            View on Amazon
          </a>
        </div>
      </div>
    </main>
  );
}
