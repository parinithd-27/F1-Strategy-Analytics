import Link from "next/link";

export default function NotFound() {
  return (
    <>
      <h1 className="page-title">Race not found</h1>
      <p>
        <Link href="/">Back to all races</Link>
      </p>
    </>
  );
}
