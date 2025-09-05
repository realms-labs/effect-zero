/**
 * Prefix a string with the package name.
 */
export const prefixId = <S extends string>(name: S) => `interop/${name}` as const;
