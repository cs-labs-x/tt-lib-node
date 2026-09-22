import { createHash } from 'node:crypto'

/**
 * Small helpers every service reaches for. They live here so that a rule
 * ("a station code is three upper-case letters") is decided once, not once
 * per service.
 */

/** "12.34" → 1234 minor units. Throws on anything that is not a plain decimal. */
export function parseMoney(text: string): number {
  const m = /^\s*(-?)(\d+)(?:\.(\d{1,2}))?\s*$/.exec(text)
  if (!m) throw new Error(`not a money amount: ${text}`)
  const [, sign, whole, frac = ''] = m
  const minor = Number(whole) * 100 + Number(frac.padEnd(2, '0'))
  return sign ? -minor : minor
}

/** 1234, "EUR" → "12.34 EUR". */
export function formatMoney(minor: number, currency = 'EUR'): string {
  const sign = minor < 0 ? '-' : ''
  const abs = Math.abs(Math.round(minor))
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, '0')} ${currency}`
}

/** "  mad-01 " → "MAD01": station codes are upper-case alphanumerics, nothing else. */
export function normalizeStationCode(code: string): string {
  return code.toUpperCase().replace(/[^A-Z0-9]/g, '')
}

/** A short, stable digest of the given parts, for keys and fingerprints. */
export function stableHash(...parts: string[]): string {
  return createHash('sha256').update(parts.join('|')).digest('hex').slice(0, 16)
}

/** Hours from `now` until `at`; negative when `at` is in the past. */
export function hoursUntil(at: Date, now: Date = new Date()): number {
  return (at.getTime() - now.getTime()) / 3_600_000
}

/** Exponential backoff with a 30 s ceiling: 200, 400, 800 … ms. */
export function backoffDelayMs(attempt: number): number {
  return Math.min(30_000, 200 * 2 ** Math.max(0, attempt))
}

/** Masks e-mail addresses and phone numbers in free text before it is logged. */
export function redactPii(text: string): string {
  return text
    .replace(/[^\s@]+@[^\s@]+\.[^\s@]+/g, '[email]')
    .replace(/\+?\d[\d\s-]{6,}\d/g, '[phone]')
}

/** Trims and collapses whitespace. Several services keep a local `normalize` of their own; this is the shared one. */
export function normalize(text: string): string {
  return text.trim().replace(/\s+/g, ' ')
}
