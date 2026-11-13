// Code borrowed from:
// https://github.com/rocicorp/mono/blob/d489a3218ad603b3fd65e33cec76ae77cea7c7af/packages/zero-react/src/use-query.tsx (zero/v1.12.0)

import type { ErroredQuery, HumanReadable, QueryErrorDetails, ResultType } from "@rocicorp/zero";
import type { QueryResult } from "@rocicorp/zero/react";

const emptyArray: unknown[] = [];

const resultTypeUnknown = { type: "unknown" } as const;
const resultTypeCached = { type: "cached" } as const;
const resultTypeComplete = { type: "complete" } as const;
const resultTypeError = {type: 'error'} as const;

const emptySnapshotSingularUnknown = [undefined, resultTypeUnknown] as const;
const emptySnapshotSingularCached = [undefined, resultTypeCached] as const;
const emptySnapshotSingularComplete = [undefined, resultTypeComplete] as const;
const emptySnapshotSingularErrorUnknown = [undefined, resultTypeError] as const;
const emptySnapshotPluralUnknown = [emptyArray, resultTypeUnknown] as const;
const emptySnapshotPluralCached = [emptyArray, resultTypeCached] as const;
const emptySnapshotPluralComplete = [emptyArray, resultTypeComplete] as const;
const emptySnapshotErrorUnknown = [emptyArray, resultTypeError] as const;

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
      case 'cached':
        return emptySnapshotSingularCached as unknown as QueryResult<TReturn>;
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
      case 'cached':
        return emptySnapshotPluralCached as unknown as QueryResult<TReturn>;
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
    case 'cached':
      return [data, resultTypeCached];
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
