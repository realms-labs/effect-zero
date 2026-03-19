// Code borrowed from:
// https://github.com/rocicorp/mono/blob/8e0f600fb3a9185facf60cfd4971d260b266690e/packages/zero-react/src/use-query.tsx
// https://github.com/rocicorp/mono/blob/8e0f600fb3a9185facf60cfd4971d260b266690e/packages/shared/src/deep-clone.ts

import type { ErroredQuery, HumanReadable, JSONValue, QueryErrorDetails, ReadonlyJSONValue, ResultType } from "@rocicorp/zero";
import type { QueryResult } from "@rocicorp/zero/react";

const emptyArray: unknown[] = [];

const resultTypeUnknown = { type: "unknown" } as const;
const resultTypeComplete = { type: "complete" } as const;
const resultTypeError = {type: 'error'} as const;

const emptySnapshotSingularUnknown = [undefined, resultTypeUnknown] as const;
const emptySnapshotSingularComplete = [undefined, resultTypeComplete] as const;
const emptySnapshotSingularErrorUnknown = [undefined, resultTypeError] as const;
const emptySnapshotPluralUnknown = [emptyArray, resultTypeUnknown] as const;
const emptySnapshotPluralComplete = [emptyArray, resultTypeComplete] as const;
const emptySnapshotErrorUnknown = [emptyArray, resultTypeError] as const;

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
  resultType: ResultType,
  retryFn: () => void,
  error?: ErroredQuery,
): QueryResult<TReturn> {
  if (singular && data === undefined) {
    switch (resultType) {
      case 'error':
        if (error) {
          return [
            undefined,
            makeError(retryFn, error),
          ] as unknown as QueryResult<TReturn>;
        }
        return emptySnapshotSingularErrorUnknown as unknown as QueryResult<TReturn>;
      case 'complete':
        return emptySnapshotSingularComplete as unknown as QueryResult<TReturn>;
      case 'unknown':
        return emptySnapshotSingularUnknown as unknown as QueryResult<TReturn>;
    }
  }

  if (!singular && (data as unknown[]).length === 0) {
    switch (resultType) {
      case 'error':
        if (error) {
          return [
            emptyArray,
            makeError(retryFn, error),
          ] as unknown as QueryResult<TReturn>;
        }
        return emptySnapshotErrorUnknown as unknown as QueryResult<TReturn>;
      case 'complete':
        return emptySnapshotPluralComplete as unknown as QueryResult<TReturn>;
      case 'unknown':
        return emptySnapshotPluralUnknown as unknown as QueryResult<TReturn>;
    }
  }

  switch (resultType) {
    case 'error':
      if (error) {
        return [data, makeError(retryFn, error)];
      }
      return [
        data,
        makeError(retryFn, {
          error: 'app',
          id: 'unknown',
          name: 'unknown',
          message: 'An unknown error occurred',
        }),
      ];
    case 'complete':
      return [data, resultTypeComplete];
    case 'unknown':
      return [data, resultTypeUnknown];
  }
}

function makeError(retry: () => void, error: ErroredQuery): QueryErrorDetails {
  const message = error.message ?? 'An unknown error occurred';
  return {
    type: 'error',
    retry,
    refetch: retry,
    error: {
      type: error.error,
      message,
      ...(error.details ? {details: error.details} : {}),
    },
  };
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
