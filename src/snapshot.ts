// Code borrowed from:
// https://github.com/rocicorp/mono/blob/30f209f2946b4cf2cd2dee459849351498f11308/packages/zero-react/src/use-query.tsx
// https://github.com/rocicorp/mono/blob/30f209f2946b4cf2cd2dee459849351498f11308/packages/shared/src/deep-clone.ts#L4

import type { JSONValue, ReadonlyJSONValue } from "@rocicorp/zero";
import type { HumanReadable, QueryResult } from "@rocicorp/zero/react";

const emptyArray: unknown[] = [];

const resultTypeUnknown = { type: "unknown" } as const;
const resultTypeComplete = { type: "complete" } as const;

const emptySnapshotSingularUnknown = [undefined, resultTypeUnknown] as const;
const emptySnapshotSingularComplete = [undefined, resultTypeComplete] as const;
const emptySnapshotPluralUnknown = [emptyArray, resultTypeUnknown] as const;
const emptySnapshotPluralComplete = [emptyArray, resultTypeComplete] as const;

export function getDefaultSnapshot<TReturn>(singular: boolean): QueryResult<TReturn> {
  return (singular ? emptySnapshotSingularUnknown : emptySnapshotPluralUnknown) as QueryResult<TReturn>;
}

/**
 * Returns a new snapshot or one of the empty predefined ones. Returning the
 * predefined ones is important to prevent unnecessary re-renders in React.
 */
export function getSnapshot<TReturn>(
  singular: boolean,
  data: HumanReadable<TReturn>,
  resultType: string,
): QueryResult<TReturn> {
  if (singular && data === undefined) {
    return (resultType === "complete"
      ? emptySnapshotSingularComplete
      : emptySnapshotSingularUnknown) as unknown as QueryResult<TReturn>;
  }

  if (!singular && (data as unknown[]).length === 0) {
    return (
      resultType === "complete" ? emptySnapshotPluralComplete : emptySnapshotPluralUnknown
    ) as QueryResult<TReturn>;
  }

  return [data, resultType === "complete" ? resultTypeComplete : resultTypeUnknown];
}

export function deepClone(value: ReadonlyJSONValue): JSONValue {
  const seen: Array<ReadonlyJSONValue> = [];
  return internalDeepClone(value, seen);
}

function internalDeepClone(value: ReadonlyJSONValue, seen: Array<ReadonlyJSONValue>): JSONValue {
  switch (typeof value) {
    case "boolean":
    case "number":
    case "string":
    case "undefined":
      return value;
    case "object": {
      if (value === null) {
        return null;
      }
      if (seen.includes(value)) {
        throw new Error("Cyclic object");
      }
      seen.push(value);
      if (Array.isArray(value)) {
        const rv = value.map((v) => internalDeepClone(v, seen));
        seen.pop();
        return rv;
      }

      const obj: JSONValue = {};

      for (const k in value) {
        if (Object.hasOwn(value, k)) {
          const v = (value as Record<string, ReadonlyJSONValue>)[k];
          if (v !== undefined) {
            obj[k] = internalDeepClone(v, seen);
          }
        }
      }
      seen.pop();
      return obj;
    }

    default:
      throw new Error(`Invalid type: ${typeof value}`);
  }
}
