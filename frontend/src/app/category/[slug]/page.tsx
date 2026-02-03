import { fetchDeals } from "../../../lib/api";

export default async function CategoryPage({ params }: { params: { slug: string } }) {
  const { slug } = params;
  const data = await fetchDeals(slug);
  const items = data.items || [];

  return (
    <main>
      <h2>Category: {slug}</h2>
      <div style={{ display: "grid", gap: 12 }}>
        {items.map((d: any) => (
          <div key={`${d.asin}-${d.category}`} style={{ border: "1px solid #ddd", borderRadius: 10, padding: 12 }}>
            <div style={{ fontSize: 12, opacity: 0.8 }}>
              Score {Math.round(d.score ?? 0)} • Confidence {Math.round(d.confidence ?? 0)}
            </div>
            <a href={`/deal/${d.asin}`} style={{ fontWeight: 700 }}>
              {d.title}
            </a>
            <div style={{ fontSize: 13, marginTop: 6 }}>
              Now: <b>£{(d.price_current ?? 0).toFixed(2)}</b> • 90d median: £{(d.price_median_90d ?? 0).toFixed(2)} •
              Drop: <b>{Math.round((d.discount_pct_90d ?? 0) * 100)}%</b>
            </div>
            <div style={{ marginTop: 8 }}>
              <a href={d.amazon_url} target="_blank">
                View on Amazon
              </a>
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
