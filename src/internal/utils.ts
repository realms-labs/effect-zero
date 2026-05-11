/**
 * Prefix a string with the package name.
 */
export const prefixId = <S extends string>(name: S) => `effect-zero/${name}` as const;

/**
 * Zero unconditionally serializes arg-less mutator calls as `null` on the wire
 * (https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-client/src/client/custom.ts).
 * v3 `Schema.Void` happened to accept any input; v4 `Schema.Void` is strict
 * `=== undefined`, so we renormalize before decoding.
 */
export const normalizeArgs = (args: unknown): unknown => (args === null ? undefined : args);
