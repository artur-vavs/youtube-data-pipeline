import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Watchlist Analytics — YouTube",
  description:
    "Compare o seu canal com os concorrentes monitorados e acompanhe as métricas da camada gold.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  );
}
