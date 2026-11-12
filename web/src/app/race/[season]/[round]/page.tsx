import type { Metadata } from "next";
import { notFound } from "next/navigation";

import { RaceDashboard } from "@/components/RaceDashboard";
import { parseRaceParams } from "@/lib/params";

type Params = Promise<{ season: string; round: string }>;

export async function generateMetadata({ params }: { params: Params }): Promise<Metadata> {
  const { season, round } = await params;
  return { title: `${season} R${round}` };
}

export default async function RacePage({ params }: { params: Params }) {
  const { season, round } = await params;
  const parsed = parseRaceParams(season, round);
  if (parsed === null) notFound();
  return <RaceDashboard season={parsed.season} round={parsed.round} />;
}
