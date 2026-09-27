# REST/GraphQL API layer — "App consumption"

Fabric's **API for GraphQL** puts a secured GraphQL endpoint over lakehouse/warehouse data with no
server to run. An app (your Angular/Node background fits perfectly here) calls it with a Microsoft
Entra token.

## 1. Create the API
Workspace → **New item → API for GraphQL** → name `api_retail` → **Select data source** →
`lh_retail` SQL analytics endpoint → tick:
* `vw_customer_360` (view), `gold_churn_scores`, `vw_sales_daily`, `vw_inventory_alerts`

Fabric generates queries like `vw_customer_360s(filter:…, first:…)`. Test in the built-in editor:
```graphql
query HighRisk {
  vw_customer_360s(
    filter: { risk_band: { eq: "High" } }
    orderBy: { churn_probability: DESC }
    first: 10
  ) {
    items { customer_id customer_name loyalty_tier churn_probability lifetime_revenue_inr }
  }
}
```
Copy the **endpoint URL** (toolbar → Copy endpoint).

> Exact generated type/field names can differ slightly (pluralisation, casing) — check the
> schema explorer and adjust the query text in `client/index.mjs` if needed.

## 2. Node.js client
```bash
cd api/client
npm install
set FABRIC_GRAPHQL_ENDPOINT=https://....graphql.fabric.microsoft.com/v1/workspaces/.../graphqlapis/.../graphql
node index.mjs          # opens a browser once to sign in with your Fabric user
```
It prints the 10 highest-risk customers — the same data an Angular dashboard or CRM integration
would consume. Extending this into an Express endpoint (`GET /api/customers/at-risk`) is a nice
way to link this project to your MEAN-stack experience.
