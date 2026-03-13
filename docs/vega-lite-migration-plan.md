# Vega-Lite Visualization Migration Plan

## Context

The finance-query-agent currently returns rigid chart specs (4 fixed types: `pie`, `bar`, `line`, `grouped_bar`) as custom Pydantic models. Adding a new chart type requires changes to backend models, LLM prompt, _and_ frontend rendering.

**Goal:** Replace the custom chart schema with [Vega-Lite v5](https://vega.github.io/vega-lite/) JSON specs. This is the same approach Snowflake Cortex uses, and the de facto standard for LLM-generated portable chart specs.

**Scope:** Only the AI chat assistant visualizations. This does NOT affect any hand-built dashboard charts in MPI.

---

## Architecture (Before → After)

### Before

```
Main Agent → decides final_answer_with_chart
  → Viz Agent (gpt-4.1-mini) receives question + SQL results
  → outputs _VisualizationOutputLLM { charts: [PieChartSpec | BarChartSpec | ...] }
  → backend returns custom JSON to MPI
  → MPI frontend has per-type rendering logic (one component per chart type)
```

### After

```
Main Agent → decides final_answer_with_chart (no change)
  → Viz Agent receives question + SQL results + data field names
  → outputs a simplified chart intent (type, fields, aggregation, sort)
  → backend expands intent into a full Vega-Lite v5 JSON spec (theming, defaults, data embedding)
  → MPI frontend renders ANY spec with vega-embed (one component, all chart types)
```

---

## Part 1 — Finance Query Agent (Backend)

Owner: this repo (`finance-query-agent`)

### 1.1 Define a simplified chart intent schema

Replace the current 4 chart-type Pydantic models with a single `ChartIntent` the LLM outputs:

```python
class ChartIntent(BaseModel):
    """What the LLM decides — minimal, easy to validate."""
    chart_type: Literal["bar", "line", "pie", "area", "scatter", "heatmap", "stacked_bar", "grouped_bar"]
    title: str
    currency: str | None = None
    x_field: str          # column name from query results
    y_field: str          # column name from query results
    color_field: str | None = None   # optional grouping/series field
    sort: Literal["ascending", "descending", "none"] = "none"
    # For grouped_bar: series labels
    series_labels: list[str] | None = None
```

This is the **simplified intermediate format** approach (chat2plot pattern). The LLM only picks chart type + maps data fields. It never writes Vega-Lite directly (error-prone).

### 1.2 Build a Vega-Lite spec builder

New module: `src/finance_query_agent/vega_builder.py`

Responsibilities:
- Takes a `ChartIntent` + the raw query result rows
- Produces a valid Vega-Lite v5 JSON spec (`dict`)
- Embeds the data inline (`{"values": [...]}`)
- Applies consistent theming (colors, fonts, sizing)
- Handles per-chart-type mark and encoding logic
- Computes pie percentages via Vega-Lite transforms (no manual computation)

Example output:
```json
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "Spending by Category (USD)",
  "data": {"values": [{"category": "Food", "amount": 320.5}, ...]},
  "mark": "bar",
  "encoding": {
    "x": {"field": "category", "type": "nominal", "sort": "-y"},
    "y": {"field": "amount", "type": "quantitative"},
    "color": {"field": "category", "type": "nominal"}
  }
}
```

### 1.3 Update the viz agent prompt

- Remove the rigid chart type definitions from the system prompt
- Instead, tell the LLM the available `chart_type` values and the column names from the query results
- Use structured output (`ChartIntent`) — same Pydantic AI pattern as today
- Keep the rules: one chart per currency, skip single-row results, max 8 pie slices

### 1.4 Update `AgentResponse.visualizations`

Change the type from `list[ChartSpec] | None` to `list[dict] | None` where each dict is a full Vega-Lite spec. Alternatively, wrap it:

```python
class VegaLiteChart(BaseModel):
    spec: dict[str, Any]  # Full Vega-Lite v5 JSON

class AgentResponse(BaseModel):
    ...
    visualizations: list[VegaLiteChart] | None = None
```

### 1.5 Delete old chart models

Remove:
- `src/finance_query_agent/schemas/charts.py` (PieChartSpec, BarChartSpec, LineChartSpec, GroupedBarChartSpec, etc.)
- All LLM-specific chart schemas in `visualization.py` (_PieSliceLLM, _PieChartSpecLLM, etc.)
- The `_to_chart_spec` conversion function

### 1.6 Update tests

- Update `tests/test_visualization.py` to assert Vega-Lite spec output
- Test the vega builder with various chart intents + data shapes
- Verify `should_visualize` logic remains unchanged

---

## Part 2 — MPI Frontend

Owner: MPI team (`my_personal_incomes_ai`)

### 2.1 Install vega-embed

```bash
npm install vega vega-lite vega-embed
# or
yarn add vega vega-lite vega-embed
```

Bundle impact: ~300KB (vega + vega-lite + vega-embed).

### 2.2 Create a generic `<VegaChart />` component

```tsx
import vegaEmbed from 'vega-embed';
import { useRef, useEffect } from 'react';

interface VegaChartProps {
  spec: Record<string, unknown>;
  className?: string;
}

export function VegaChart({ spec, className }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const result = vegaEmbed(containerRef.current, spec, {
      actions: false,    // hide export/source buttons
      renderer: 'svg',   // or 'canvas'
      theme: 'dark',     // match MPI theme if applicable
    });
    return () => { result.then(r => r.finalize()); };
  }, [spec]);

  return <div ref={containerRef} className={className} />;
}
```

### 2.3 Replace per-type chart components

Remove any existing chart-type-specific components (e.g., `<PieChart>`, `<BarChart>`) used for AI responses. Replace with:

```tsx
{response.visualizations?.map((viz, i) => (
  <VegaChart key={i} spec={viz.spec} />
))}
```

This single component renders **any** chart type the backend sends — no frontend changes needed when new chart types are added.

### 2.4 Handle the response shape change

The `visualizations` field changes from:
```json
[{"chart_type": "pie", "title": "...", "slices": [...]}]
```
to:
```json
[{"spec": {"$schema": "https://vega.github.io/schema/vega-lite/v5.json", ...}}]
```

Update the TypeScript types and any parsing logic accordingly.

---

## Migration Strategy

### Option A: Big bang (simpler, recommended)

1. Backend ships Vega-Lite specs in `visualizations`
2. MPI deploys `<VegaChart />` at the same time
3. Coordinate a single deployment

### Option B: Gradual (if coordination is hard)

1. Backend adds a new field `visualizations_v2: list[VegaLiteChart] | None` alongside the existing `visualizations`
2. MPI switches to reading `visualizations_v2` and rendering with `<VegaChart />`
3. Once confirmed, backend removes `visualizations` and renames `visualizations_v2`

---

## Benefits

| Before | After |
|--------|-------|
| 4 fixed chart types | 8+ chart types, extensible without code changes |
| Frontend needs per-type rendering | One `<VegaChart />` component renders everything |
| Adding a chart type = backend + prompt + frontend | Adding a chart type = add to `chart_type` Literal + vega builder case |
| No interactivity | Built-in tooltips, zoom, brush selection via vega-embed |
| Custom percentage computation | Vega-Lite transforms handle aggregation natively |

## New chart types unlocked (no frontend changes needed)

- Area charts (cumulative spending over time)
- Scatter plots (transaction amount vs frequency)
- Heatmaps (spending intensity by day-of-week × hour)
- Stacked bar (category breakdown within monthly totals)
- Any future Vega-Lite compatible visualization
