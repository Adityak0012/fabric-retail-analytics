// Query the Fabric API for GraphQL with a signed-in Entra user.
// Usage: FABRIC_GRAPHQL_ENDPOINT=<url> node index.mjs
import { InteractiveBrowserCredential } from "@azure/identity";

const endpoint = process.env.FABRIC_GRAPHQL_ENDPOINT;
if (!endpoint) {
  console.error("Set FABRIC_GRAPHQL_ENDPOINT (API for GraphQL > Copy endpoint).");
  process.exit(1);
}

const credential = new InteractiveBrowserCredential({
  // Optional: set AZURE_TENANT_ID if your Fabric user lives in a specific tenant
  tenantId: process.env.AZURE_TENANT_ID,
});
const { token } = await credential.getToken("https://api.fabric.microsoft.com/.default");

const query = `
  query HighRisk($n: Int!) {
    vw_customer_360s(
      filter: { risk_band: { eq: "High" } }
      orderBy: { churn_probability: DESC }
      first: $n
    ) {
      items { customer_id customer_name loyalty_tier country churn_probability lifetime_revenue_inr }
    }
  }`;

const res = await fetch(endpoint, {
  method: "POST",
  headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
  body: JSON.stringify({ query, variables: { n: 10 } }),
});
const body = await res.json();
if (!res.ok || body.errors) {
  console.error("GraphQL error:", JSON.stringify(body.errors ?? body, null, 2));
  process.exit(1);
}
console.table(
  body.data.vw_customer_360s.items.map((c) => ({
    id: c.customer_id,
    name: c.customer_name,
    tier: c.loyalty_tier,
    country: c.country,
    churn: Number(c.churn_probability).toFixed(2),
    lifetime_revenue_inr: Math.round(c.lifetime_revenue_inr ?? 0),
  }))
);
