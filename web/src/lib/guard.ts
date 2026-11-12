/** Small runtime guards for untrusted JSON (API responses, SSE messages). No schema library. */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Obj = Record<string, unknown>;

export const isObj = (v: unknown): v is Obj =>
  typeof v === "object" && v !== null && !Array.isArray(v);

export const isFiniteNumber = (v: unknown): v is number =>
  typeof v === "number" && Number.isFinite(v);

export const isInt = (v: unknown): v is number => isFiniteNumber(v) && Number.isInteger(v);

export const isStr = (v: unknown): v is string => typeof v === "string";

export const isBool = (v: unknown): v is boolean => typeof v === "boolean";

/** ``v`` when it passes ``check``, ``null`` when it is null, otherwise ``undefined`` (invalid). */
export function nullable<T>(v: unknown, check: (x: unknown) => x is T): T | null | undefined {
  if (v === null) return null;
  return check(v) ? v : undefined;
}

export const DRIVER_ID = /^[a-z0-9_]+$/;
export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const isDriverId = (v: unknown): v is string => isStr(v) && DRIVER_ID.test(v);
