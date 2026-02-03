export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "Arial, sans-serif", margin: 0, padding: 0 }}>
        <div style={{ maxWidth: 1100, margin: "0 auto", padding: 16 }}>
          <header style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
            <h1 style={{ margin: 0 }}>Amazon UK Deals</h1>
            <nav style={{ display: "flex", gap: 12 }}>
              <a href="/">Top</a>
              <a href="/category/home">Home</a>
              <a href="/category/kitchen">Kitchen</a>
              <a href="/category/diy">DIY</a>
              <a href="/category/electrical">Electrical</a>
              <a href="/category/toys">Toys</a>
            </nav>
          </header>
          <hr />
          {children}
          <hr />
          <footer style={{ fontSize: 12, opacity: 0.8 }}>
            <div>Disclosure: As an Amazon Associate, this site may earn from qualifying purchases.</div>
          </footer>
        </div>
      </body>
    </html>
  );
}
