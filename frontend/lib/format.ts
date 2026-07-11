const compact = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const full = new Intl.NumberFormat("pt-BR");

export function formatCompact(value: number | null | undefined): string {
  if (value == null) return "—";
  return compact.format(value);
}

export function formatInt(value: number | null | undefined): string {
  if (value == null) return "—";
  return full.format(value);
}

export function formatPct(ratio: number | null | undefined): string {
  if (ratio == null) return "—";
  return `${(ratio * 100).toFixed(2)}%`;
}

export function formatSigned(value: number | null | undefined): string {
  if (value == null) return "—";
  const sign = value > 0 ? "+" : "";
  return `${sign}${full.format(value)}`;
}

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleDateString("pt-BR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}
