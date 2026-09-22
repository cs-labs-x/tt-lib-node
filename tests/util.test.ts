import { describe, it, expect } from 'vitest'
import { parseMoney, formatMoney, normalizeStationCode, stableHash, hoursUntil, backoffDelayMs, redactPii, normalize } from '../src/util'

describe('util', () => {
  it('parses and formats money in minor units', () => {
    expect(parseMoney('12.34')).toBe(1234)
    expect(parseMoney('7')).toBe(700)
    expect(parseMoney('-0.5')).toBe(-50)
    expect(() => parseMoney('12,34')).toThrow()
    expect(formatMoney(1234)).toBe('12.34 EUR')
    expect(formatMoney(-5, 'GBP')).toBe('-0.05 GBP')
  })

  it('normalises station codes to upper-case alphanumerics', () => {
    expect(normalizeStationCode(' mad-01 ')).toBe('MAD01')
  })

  it('hashes stably', () => {
    expect(stableHash('a', 'b')).toBe(stableHash('a', 'b'))
    expect(stableHash('a', 'b')).not.toBe(stableHash('ab'))
    expect(stableHash('x')).toHaveLength(16)
  })

  it('measures hours until a date', () => {
    const now = new Date('2026-01-01T00:00:00Z')
    expect(hoursUntil(new Date('2026-01-01T06:00:00Z'), now)).toBe(6)
    expect(hoursUntil(new Date('2025-12-31T23:00:00Z'), now)).toBe(-1)
  })

  it('backs off exponentially with a ceiling', () => {
    expect([0, 1, 2, 3].map(backoffDelayMs)).toEqual([200, 400, 800, 1600])
    expect(backoffDelayMs(20)).toBe(30_000)
  })

  it('redacts e-mails and phones', () => {
    expect(redactPii('mail ana@example.com or +34 600 123 456')).toBe('mail [email] or [phone]')
  })

  it('normalises whitespace', () => {
    expect(normalize('  a   b \n c ')).toBe('a b c')
  })
})
