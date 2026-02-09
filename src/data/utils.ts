import { CoreApp, DataFrame, DataQueryRequest, DataQueryResponse } from '@grafana/data';
import {
  ColumnHint,
  FilterOperator,
  OrderByDirection,
  QueryBuilderOptions,
  QueryType,
  SelectedColumn,
  StringFilter,
} from 'types/queryBuilder';
import { CHBuilderQuery, CHQuery, EditorType } from 'types/sql';
import { Datasource } from './CHDatasource';
import { pluginVersion } from 'utils/version';
import { logColumnHintsToAlias, generateSql } from './sqlGenerator';
import otel from 'otel';

/**
 * Returns true if the builder options contain enough information to start showing a query
 */
export const isBuilderOptionsRunnable = (builderOptions: QueryBuilderOptions): boolean => {
  return (
    (builderOptions.columns?.length || 0) > 0 ||
    (builderOptions.filters?.length || 0) > 0 ||
    (builderOptions.orderBy?.length || 0) > 0 ||
    (builderOptions.aggregates?.length || 0) > 0 ||
    (builderOptions.groupBy?.length || 0) > 0
  );
};

/**
 * Converts QueryBuilderOptions to Grafana format
 * src: https://github.com/grafana/sqlds/blob/main/query.go#L20
 */
export const mapQueryBuilderOptionsToGrafanaFormat = (t?: QueryBuilderOptions): number => {
  switch (t?.queryType) {
    case QueryType.Table:
      return 1;
    case QueryType.Logs:
      return 2;
    case QueryType.TimeSeries:
      return 0;
    case QueryType.Traces:
      return t.meta?.isTraceIdMode ? 3 : 1;
    default:
      return 1 << 8; // an unused u32, defaults to timeseries/graph on plugin backend.
  }
};

/**
 * Converts QueryType to Grafana format
 * src: https://github.com/grafana/sqlds/blob/main/query.go#L20
 */
export const mapQueryTypeToGrafanaFormat = (t?: QueryType): number => {
  switch (t) {
    case QueryType.Table:
      return 1;
    case QueryType.Logs:
      return 2;
    case QueryType.TimeSeries:
      return 0;
    case QueryType.Traces:
      return 3;
    default:
      return 1 << 8; // an unused u32, defaults to timeseries/graph on plugin backend.
  }
};

/**
 * Converts Grafana format to builder QueryType
 * src: https://github.com/grafana/sqlds/blob/main/query.go#L20
 */
export const mapGrafanaFormatToQueryType = (f?: number): QueryType => {
  switch (f) {
    case 0:
      return QueryType.TimeSeries;
    case 1:
      return QueryType.Table;
    case 2:
      return QueryType.Logs;
    case 3:
      return QueryType.Traces;
    default:
      return QueryType.Table;
  }
};

/**
 * Manipulates column array in-place to include column hints, loosely matched by the provided column hint map.
 */
export const tryApplyColumnHints = (columns: SelectedColumn[], hintsToColumns?: Map<ColumnHint, string>) => {
  const columnsToHints: Map<string, ColumnHint> = new Map();
  if (hintsToColumns) {
    hintsToColumns.forEach((name, hint) => {
      columnsToHints.set(name.toLowerCase().trim(), hint);
    });
  }

  for (const column of columns) {
    if (column.hint) {
      continue;
    }

    const name = column.name.toLowerCase().trim();
    const alias = column.alias?.toLowerCase().trim() || '';

    const hint = columnsToHints.get(name) || columnsToHints.get(alias);
    if (hint) {
      column.hint = hint;
      continue;
    }

    if (name.includes('time')) {
      column.hint = ColumnHint.Time;
    }
  }
};

/**
 * Converts label into sql-style column name.
 * Example: "Test Column" -> "test_column"
 */
export const columnLabelToPlaceholder = (label: string) => label.toLowerCase().replace(/ /g, '_');

/**
 * Mutates the DataQueryResponse to include trace/log links on the traceID field.
 * The link will open a second query editor in split view
 * on the explore page with the selected trace ID.
 *
 * Requires defaults to be configured when crossing query types.
 */
export const transformQueryResponseWithTraceAndLogLinks = (
  datasource: Datasource,
  req: DataQueryRequest<CHQuery>,
  res: DataQueryResponse
): DataQueryResponse => {
  res.data.forEach((frame: DataFrame) => {
    const originalQuery = req.targets.find((t) => t.refId === frame.refId) as CHBuilderQuery;
    // Only filter empty fields if the option is enabled
    if (originalQuery?.builderOptions?.meta?.filterEmptyFields ?? true) {
      filterEmpty(frame);
    }
    if (!originalQuery) {
      return;
    }

    const traceField = frame.fields.find(
      (field) => field.name.toLowerCase() === 'traceid' || field.name.toLowerCase() === 'trace_id'
    );
    if (!traceField) {
      return;
    }

    // Get the configured TraceId column name for use in both trace and logs queries
    const defaultLogsColumns = datasource.getDefaultLogsColumns();
    // Use traces config traceIdColumn if available, otherwise fallback to logs default
    const traceIdColumnName = datasource.getTracesTraceIdColumn() || defaultLogsColumns.get(ColumnHint.TraceId) || 'TraceId';

    const traceIdQuery: CHBuilderQuery = {
      datasource: datasource,
      editorType: EditorType.Builder,
      /**
       * Evil bug:
       * The rawSql value might contain time filters such as $__fromTime and $__toTime.
       * Grafana sees these time range filters as data links and will refuse to enable the traceID link if these are present.
       * Set rawSql to empty since it gets regenerated when the panel renders anyway.
       */
      rawSql: '',
      builderOptions: {} as QueryBuilderOptions,
      pluginVersion,
      refId: 'Trace ID',
    };

    if (
      originalQuery.editorType === EditorType.Builder &&
      originalQuery.builderOptions.queryType === QueryType.Traces
    ) {
      // Copy fields directly from trace search

      traceIdQuery.builderOptions = {
        ...originalQuery.builderOptions,
        filters: [], // Clear filters and orderBy since it's an exact ID lookup
        orderBy: [],
        meta: {
          ...originalQuery.builderOptions.meta,
          minimized: true,
          isTraceIdMode: true,
          traceId: '${__value.raw}',
        },
      };
    } else {
      // Create new query based on trace defaults

      const otelVersion = datasource.getTraceOtelVersion();
      const otelConfig = otel.getVersion(otelVersion);
      const traceEventsColumnPrefix = datasource.getDefaultTraceEventsColumnPrefix();
      const traceLinksColumnPrefix = datasource.getDefaultTraceLinksColumnPrefix();
      const options: QueryBuilderOptions = {
        database:
          datasource.getDefaultTraceDatabase() ||
          traceIdQuery.builderOptions.database ||
          datasource.getDefaultDatabase(),
        table: datasource.getDefaultTraceTable() || datasource.getDefaultTable() || traceIdQuery.builderOptions.table,
        queryType: QueryType.Traces,
        columns: [],
        filters: [],
        orderBy: [],
        meta: {
          minimized: true,
          isTraceIdMode: true,
          traceId: '${__value.raw}',
          traceDurationUnit: datasource.getDefaultTraceDurationUnit(),
          otelEnabled: Boolean(otelVersion),
          otelVersion: otelVersion,
          traceEventsColumnPrefix: traceEventsColumnPrefix,
          traceLinksColumnPrefix: traceLinksColumnPrefix,
        },
      };

      if (otelConfig?.traceColumnMap) {
        options.columns = Array.from(otelConfig.traceColumnMap, ([hint, name]) => ({ name, hint }));
      } else {
        const defaultColumns = datasource.getDefaultTraceColumns();
        for (let [hint, colName] of defaultColumns) {
          options.columns!.push({ name: colName, hint });
        }
      }

      traceIdQuery.builderOptions = options;
    }

    const traceLogsQuery: CHBuilderQuery = {
      datasource: datasource,
      editorType: EditorType.Builder,
      rawSql: '',
      builderOptions: {} as QueryBuilderOptions,
      pluginVersion,
      refId: 'Trace Logs',
    };

    if (originalQuery.editorType === EditorType.Builder && originalQuery.builderOptions.queryType === QueryType.Logs) {
      // Copy fields directly from log search
      traceLogsQuery.builderOptions = {
        ...originalQuery.builderOptions,
        filters: [
          {
            type: 'string',
            operator: FilterOperator.Equals,
            filterType: 'custom',
            key: traceIdColumnName,
            hint: ColumnHint.TraceId,
            condition: 'AND',
            value: '${__value.raw}',
          } as StringFilter,
        ],
        orderBy: [{ name: '', hint: ColumnHint.Time, dir: OrderByDirection.ASC }],
        meta: {
          ...originalQuery.builderOptions.meta,
          minimized: true,
        },
      };
    } else {
      // Create new query based on log defaults

      const otelVersion = datasource.getLogsOtelVersion();
      const options: QueryBuilderOptions = {
        database:
          datasource.getDefaultLogsDatabase() ||
          traceLogsQuery.builderOptions.database ||
          datasource.getDefaultDatabase(),
        table: datasource.getDefaultLogsTable() || datasource.getDefaultTable() || traceLogsQuery.builderOptions.table,
        queryType: QueryType.Logs,
        columns: [],
        orderBy: [{ name: '', hint: ColumnHint.Time, dir: OrderByDirection.ASC }],
        filters: [
          {
            type: 'string',
            operator: FilterOperator.Equals,
            filterType: 'custom',
            key: traceIdColumnName,
            hint: ColumnHint.TraceId,
            condition: 'AND',
            value: '${__value.raw}',
          } as StringFilter,
        ],
        meta: {
          minimized: true,
          otelEnabled: Boolean(otelVersion),
          otelVersion: otelVersion,
        },
      };

      for (let [hint, colName] of defaultLogsColumns) {
        options.columns!.push({ name: colName, hint });
      }

      // Ensure TraceId column is in the array so filter can find it via hint lookup
      if (!options.columns!.find((c) => c.hint === ColumnHint.TraceId)) {
        options.columns!.push({ name: traceIdColumnName, hint: ColumnHint.TraceId });
      }

      traceLogsQuery.builderOptions = options;
    }

    // Generate rawSql for Dashboard mode to preserve query through serialization
    const openInNewWindow = req.app !== CoreApp.Explore;
    if (openInNewWindow) {
      traceLogsQuery.rawSql = generateSql(traceLogsQuery.builderOptions || {});
    } else {
      traceLogsQuery.rawSql = '';
    }
    traceField.config.links = [];
    traceField.config.links!.push({
      title: 'View trace',
      targetBlank: openInNewWindow,
      url: '',
      internal: {
        query: traceIdQuery,
        datasourceUid: traceIdQuery.datasource?.uid!,
        datasourceName: traceIdQuery.datasource?.type!,
        panelsState: {
          trace: {
            spanId: '${__value.raw}',
          },
        },
      },
    });
    traceField.config.links!.push({
      title: 'View logs',
      targetBlank: openInNewWindow,
      url: '',
      internal: {
        query: traceLogsQuery,
        datasourceUid: traceLogsQuery.datasource?.uid!,
        datasourceName: traceLogsQuery.datasource?.type!,
      },
    });
  });

  return res;
};

/**
 * Returns true if the dataframe contains a log label that matches the provided name.
 *
 * This function exists for the logs panel, when clicking "filter for value" on a single log row.
 * A dataframe will be provided for that single row, and we need to check the labels object to see if it
 * contains a field with that name. If it does then we can create a filter using the labels column hint.
 */
export const dataFrameHasLogLabelWithName = (frame: DataFrame | undefined, name: string): boolean => {
  if (!frame || !frame.fields || frame.fields.length === 0) {
    return false;
  }

  const logLabelsFieldName = logColumnHintsToAlias.get(ColumnHint.LogLabels);
  const field = frame.fields.find((f) => f.name === logLabelsFieldName);
  if (!field || !field.values || field.values.length < 1 || !field.values.get(0)) {
    return false;
  }

  const labels = (field.values.get(0) || {}) as object;
  const labelKeys = Object.keys(labels);

  return labelKeys.includes(name);
};

/**
 * Recursively filters out empty values from a value (used for JSON objects).
 * @example
 * ```
 * const value = { "some": "value", "empty": { "string": "", "number": 0, "boolean": false, "null": null, "undefined": undefined, "array": [], "object": {} } }
 * filterEmptyValue(value)
 * // => { "some": "value" }
 * ```
 */
/**
 * Check if a string represents an epoch/null timestamp (around 1970-01-01 00:00:00 UTC)
 * This handles various timezone representations like "1969-12-31T19:00:00-05:00"
 */
const isEpochTimestamp = (value: string): boolean => {
  // Quick check for common epoch date patterns before expensive Date parsing
  if (!value.match(/^19(69-12-31|70-01-01)/)) {
    return false;
  }
  try {
    const date = new Date(value);
    // Check if the timestamp is within 24 hours of epoch (to handle timezone variations)
    return !isNaN(date.getTime()) && Math.abs(date.getTime()) < 86400000;
  } catch {
    return false;
  }
};

const filterEmptyValue = (value: any): any => {
  console.log('filterEmptyValue', value);
  console.log('filterEmptyValue typeof', typeof value);
  if (value === null || value === undefined) {
    return undefined;
  }

  if (typeof value === "string") {
    // Try to parse as JSON if it looks like JSON
    if (value.trim().startsWith('{') || value.trim().startsWith('[')) {
      try {
        const parsed = JSON.parse(value);
        const filtered = filterEmptyValue(parsed);
        return filtered !== undefined ? JSON.stringify(filtered) : undefined;
      } catch {
        // Not valid JSON, treat as regular string
      }
    }
    // Filter empty strings, "0", and epoch timestamps
    if (value === "" || value === "0" || isEpochTimestamp(value)) {
      return undefined;
    }
    return value;
  }

  if (typeof value === "number") {
    return value === 0 ? undefined : value;
  }

  if (typeof value === "boolean") {
    return value === false ? undefined : value;
  }

  if (Array.isArray(value)) {
    const filtered = value.map(filterEmptyValue).filter((v) => v !== undefined);
    return filtered.length === 0 ? undefined : filtered;
  }

  if (typeof value === "object") {
    console.log('filterEmptyValue object', value);
    const result: any = {};
    for (const key of Object.keys(value)) {
      const filtered = filterEmptyValue(value[key]);
      if (filtered !== undefined) {
        result[key] = filtered;
      }
    }
    console.log('filterEmptyValue result', result);
    return Object.keys(result).length === 0 ? undefined : result;
  }

  return value;
};

/**
 * Filters out undefined values from DataFrame field values.
 * For JSON fields (like labels), recursively filters out empty/null values from the JSON objects.
 */
export const filterEmpty = (frame: DataFrame): void => {
  frame.fields.forEach((field) => {
    // Filter out undefined values from the field values array
    const originalValues = field.values;
    
    // Process each value: filter undefined and recursively filter empty values from JSON objects
    const processedValues: any[] = [];
    for (let i = 0; i < originalValues.length; i++) {
      const value = originalValues[i];
      if (value === undefined) {
        continue; // Skip undefined values
      }
      
      // Check if this field might be a JSON field (labels, attributes, etc.)
      // JSON fields are typically stored as objects or JSON strings
      let processedValue = value;
      if (value && typeof value === 'object' && !Array.isArray(value) && value.constructor === Object) {
        // It's a plain object (likely JSON from ClickHouse)
        processedValue = filterEmptyValue(value);
        if (processedValue !== undefined) {
          processedValues.push(processedValue);
        }
      } else if (value && typeof value === 'string' && (value.trim().startsWith('{') || value.trim().startsWith('['))) {
        // It's a string that might be JSON
        try {
          const parsed = JSON.parse(value);
          const filtered = filterEmptyValue(parsed);
          if (filtered !== undefined) {
            processedValues.push(JSON.stringify(filtered));
          }
        } catch {
          // Not valid JSON, keep as is
          processedValues.push(value);
        }
      } else {
        // Regular value, keep as is
        processedValues.push(value);
      }
    }
    
    // Always update field values with processed values
    // This handles both removed values AND modified JSON content
    field.values = processedValues as any;
  });
};