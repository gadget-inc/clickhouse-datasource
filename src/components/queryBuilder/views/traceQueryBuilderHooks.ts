import React, { useEffect, useRef } from 'react';
import { Datasource } from 'data/CHDatasource';
import otel from 'otel';
import {
  ColumnHint,
  DateFilterWithoutValue,
  Filter,
  FilterOperator,
  NumberFilter,
  OrderBy,
  OrderByDirection,
  QueryBuilderOptions,
  SelectedColumn,
  StringFilter,
  TableColumn,
} from 'types/queryBuilder';
import { BuilderOptionsReducerAction, setOptions } from 'hooks/useBuilderOptionsState';

/**
 * Loads the default configuration for new queries. (Only runs on new queries)
 */
export const useTraceDefaultsOnMount = (
  datasource: Datasource,
  isNewQuery: boolean,
  builderOptions: QueryBuilderOptions,
  builderOptionsDispatch: React.Dispatch<BuilderOptionsReducerAction>
) => {
  const didSetDefaults = useRef<boolean>(false);
  useEffect(() => {
    if (!isNewQuery || didSetDefaults.current) {
      return;
    }

    const defaultDb = datasource.getDefaultTraceDatabase() || datasource.getDefaultDatabase();
    const defaultTable = datasource.getDefaultTraceTable() || datasource.getDefaultTable();
    const defaultDurationUnit = datasource.getDefaultTraceDurationUnit();
    const otelVersion = datasource.getTraceOtelVersion();
    const defaultColumns = datasource.getDefaultTraceColumns();
    const defaultFlattenNested = datasource.getDefaultTraceFlattenNested();
    const defaultEventsColumnPrefix = datasource.getDefaultTraceEventsColumnPrefix();
    const defaultLinksColumnPrefix = datasource.getDefaultTraceLinksColumnPrefix();

    const nextColumns: SelectedColumn[] = [];
    for (let [hint, colName] of defaultColumns) {
      nextColumns.push({ name: colName, hint });
    }

    builderOptionsDispatch(
      setOptions({
        database: defaultDb,
        table: defaultTable || builderOptions.table,
        columns: nextColumns,
        meta: {
          otelEnabled: Boolean(otelVersion),
          otelVersion,
          traceDurationUnit: defaultDurationUnit,
          flattenNested: defaultFlattenNested,
          traceEventsColumnPrefix: defaultEventsColumnPrefix,
          traceLinksColumnPrefix: defaultLinksColumnPrefix,
        },
      })
    );
    didSetDefaults.current = true;
  }, [
    builderOptions.columns,
    builderOptions.orderBy,
    builderOptions.table,
    builderOptionsDispatch,
    datasource,
    isNewQuery,
  ]);
};

/**
 * Sets OTEL Trace columns automatically when OTEL is enabled
 * Does not run if OTEL is already enabled, only when it's changed.
 */
export const useOtelColumns = (
  otelEnabled: boolean,
  otelVersion: string,
  builderOptionsDispatch: React.Dispatch<BuilderOptionsReducerAction>
) => {
  const didSetColumns = useRef<boolean>(otelEnabled);
  if (!otelEnabled) {
    didSetColumns.current = false;
  }

  useEffect(() => {
    if (!otelEnabled || didSetColumns.current) {
      return;
    }

    const otelConfig = otel.getVersion(otelVersion);
    const traceColumnMap = otelConfig?.traceColumnMap;
    if (!traceColumnMap) {
      return;
    }

    const columns: SelectedColumn[] = [];
    traceColumnMap.forEach((name, hint) => {
      columns.push({ name, hint });
    });

    builderOptionsDispatch(
      setOptions({
        columns,
        meta: {
          traceDurationUnit: otelConfig.traceDurationUnit,
          flattenNested: otelConfig.flattenNested,
          traceEventsColumnPrefix: otelConfig.traceEventsColumnPrefix,
          traceLinksColumnPrefix: otelConfig.traceLinksColumnPrefix,
        },
      })
    );
    didSetColumns.current = true;
  }, [otelEnabled, otelVersion, builderOptionsDispatch]);
};

/**
 * Auto-populates column types from the table schema and detects JSON attribute columns.
 * When JSON columns are detected for TraceTags or TraceServiceTags, automatically sets useJsonAttributes.
 */
export const useColumnTypes = (
  allColumns: readonly TableColumn[],
  columns: SelectedColumn[] | undefined,
  useJsonAttributes: boolean | undefined,
  builderOptionsDispatch: React.Dispatch<BuilderOptionsReducerAction>
) => {
  const didPopulateTypes = useRef<boolean>(false);

  useEffect(() => {
    // Wait until we have both columns and schema
    if (!columns?.length || !allColumns?.length) {
      didPopulateTypes.current = false;
      return;
    }

    // Check if any columns are missing types
    const columnsNeedTypes = columns.some((c) => !c.type);
    if (!columnsNeedTypes && didPopulateTypes.current) {
      return;
    }

    // Populate types from schema
    let hasChanges = false;
    const updatedColumns = columns.map((col) => {
      if (col.type) {
        return col;
      }
      const schemaCol = allColumns.find((c) => c.name === col.name);
      if (schemaCol?.type) {
        hasChanges = true;
        return { ...col, type: schemaCol.type };
      }
      return col;
    });

    // Auto-detect JSON attributes
    let detectedJsonAttributes = false;
    if (!useJsonAttributes) {
      const tagsCol = updatedColumns.find((c) => c.hint === ColumnHint.TraceTags);
      const serviceTagsCol = updatedColumns.find((c) => c.hint === ColumnHint.TraceServiceTags);
      
      console.log('tagsCol', tagsCol);
      console.log('serviceTagsCol', serviceTagsCol);
      detectedJsonAttributes = 
        tagsCol?.type?.toLowerCase().startsWith('json') ||
        serviceTagsCol?.type?.toLowerCase().startsWith('json') ||
        detectedJsonAttributes;
    }

    if (hasChanges || detectedJsonAttributes) {
      builderOptionsDispatch(
        setOptions({
          columns: updatedColumns,
          meta: detectedJsonAttributes ? { useJsonAttributes: true } : undefined,
        })
      );
    }

    didPopulateTypes.current = true;
  }, [allColumns, columns, useJsonAttributes, builderOptionsDispatch]);
};

// Apply default filters on table change
export const useDefaultFilters = (
  table: string,
  isTraceIdMode: boolean,
  isNewQuery: boolean,
  builderOptionsDispatch: React.Dispatch<BuilderOptionsReducerAction>
) => {
  const appliedDefaultFilters = useRef<boolean>(!isNewQuery);
  const lastTable = useRef<string>(table || '');
  if (table !== lastTable.current) {
    appliedDefaultFilters.current = false;
  }

  useEffect(() => {
    if (isTraceIdMode || !table || appliedDefaultFilters.current) {
      return;
    }

    const defaultFilters: Filter[] = [
      {
        type: 'datetime',
        operator: FilterOperator.WithInGrafanaTimeRange,
        filterType: 'custom',
        key: '',
        hint: ColumnHint.Time,
        condition: 'AND',
      } as DateFilterWithoutValue, // Filter to dashboard time range
      {
        type: 'string',
        operator: FilterOperator.IsEmpty,
        filterType: 'custom',
        key: '',
        hint: ColumnHint.TraceParentSpanId,
        condition: 'AND',
        value: '',
      } as StringFilter, // Only show top level spans
      {
        type: 'UInt64',
        operator: FilterOperator.GreaterThan,
        filterType: 'custom',
        key: '',
        hint: ColumnHint.TraceDurationTime,
        condition: 'AND',
        value: 0,
      } as NumberFilter, // Only show spans where duration > 0
      {
        type: 'string',
        operator: FilterOperator.IsAnything,
        filterType: 'custom',
        key: '',
        hint: ColumnHint.TraceServiceName,
        condition: 'AND',
        value: '',
      } as StringFilter, // Placeholder service name filter for convenience
    ];

    const defaultOrderBy: OrderBy[] = [
      { name: '', hint: ColumnHint.Time, dir: OrderByDirection.DESC, default: true },
      { name: '', hint: ColumnHint.TraceDurationTime, dir: OrderByDirection.DESC, default: true },
    ];

    lastTable.current = table;
    appliedDefaultFilters.current = true;
    builderOptionsDispatch(
      setOptions({
        filters: defaultFilters,
        orderBy: defaultOrderBy,
      })
    );
  }, [table, isTraceIdMode, builderOptionsDispatch]);
};
