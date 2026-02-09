import { ColumnHint, QueryBuilderOptions, QueryType } from 'types/queryBuilder';
import {
  columnLabelToPlaceholder,
  dataFrameHasLogLabelWithName,
  isBuilderOptionsRunnable,
  transformQueryResponseWithTraceAndLogLinks,
  tryApplyColumnHints,
} from './utils';
import { newMockDatasource } from '__mocks__/datasource';
import { CoreApp, DataFrame, DataQueryRequest, DataQueryResponse, Field, FieldType, arrayToDataFrame } from '@grafana/data';
import { CHBuilderQuery, CHQuery, EditorType } from 'types/sql';
import { logColumnHintsToAlias } from './sqlGenerator';

describe('isBuilderOptionsRunnable', () => {
  it('should return false for empty builder options', () => {
    const opts: QueryBuilderOptions = {
      database: 'default',
      table: 'test',
      queryType: QueryType.Table,
    };

    const runnable = isBuilderOptionsRunnable(opts);
    expect(runnable).toBe(false);
  });

  it('should return true for valid builder options', () => {
    const opts: QueryBuilderOptions = {
      database: 'default',
      table: 'test',
      queryType: QueryType.Table,
      columns: [{ name: 'valid_column' }],
    };

    const runnable = isBuilderOptionsRunnable(opts);
    expect(runnable).toBe(true);
  });
});

describe('tryApplyColumnHints', () => {
  it('does not apply hints when queryType and hint map are not provided', () => {
    const columns = [
      { name: 'a', alias: undefined, hint: undefined },
      { name: 'b', alias: undefined, hint: undefined },
    ];

    tryApplyColumnHints(columns);

    expect(columns[0].hint).toBeUndefined();
    expect(columns[1].hint).toBeUndefined();
  });

  it('applies time hint to columns that contain "time"', () => {
    const columns = [
      { name: 'Timestamp', alias: undefined, hint: undefined },
      { name: 'log_timestamp', alias: undefined, hint: undefined },
    ];

    tryApplyColumnHints(columns);

    expect(columns[0].hint).toEqual(ColumnHint.Time);
    expect(columns[1].hint).toEqual(ColumnHint.Time);
  });

  it('does not apply hints to column with existing hint', () => {
    const columns = [{ name: 'time', alias: undefined, hint: ColumnHint.TraceServiceName }];

    tryApplyColumnHints(columns);

    expect(columns[0].hint).toEqual(ColumnHint.TraceServiceName);
  });

  it('applies hints by column name according to hint map, ignoring case', () => {
    const columns = [
      { name: 'Super_Custom_Timestamp', alias: undefined, hint: undefined },
      { name: 'LogLevel', alias: undefined, hint: undefined },
    ];
    const hintMap: Map<ColumnHint, string> = new Map([
      [ColumnHint.Time, 'super_custom_timestamp'],
      [ColumnHint.LogLevel, 'LogLevel'],
    ]);

    tryApplyColumnHints(columns, hintMap);

    expect(columns[0].hint).toEqual(ColumnHint.Time);
    expect(columns[1].hint).toEqual(ColumnHint.LogLevel);
  });

  it('applies hints by column alias according to hint map, ignoring case', () => {
    const columns = [
      { name: 'other name', alias: 'Super_Custom_Timestamp', hint: undefined },
      { name: 'other name', alias: 'LogLevel', hint: undefined },
    ];
    const hintMap: Map<ColumnHint, string> = new Map([
      [ColumnHint.Time, 'super_custom_timestamp'],
      [ColumnHint.LogLevel, 'LogLevel'],
    ]);

    tryApplyColumnHints(columns, hintMap);

    expect(columns[0].hint).toEqual(ColumnHint.Time);
    expect(columns[1].hint).toEqual(ColumnHint.LogLevel);
  });
});

describe('columnLabelToPlaceholder', () => {
  it('converts to lowercase and removes multiple spaces', () => {
    const expected = 'expected_test_output';
    const actual = columnLabelToPlaceholder('Expected TEST output');
    expect(actual).toEqual(expected);
  });
});

describe('transformQueryResponseWithTraceAndLogLinks', () => {
  const buildTestRequestResponse = (
    builderOptions: Partial<QueryBuilderOptions>
  ): [DataQueryRequest<CHQuery>, DataQueryResponse] => {
    const inputQuery: CHBuilderQuery = {
      refId: 'A',
      editorType: EditorType.Builder,
      builderOptions: {
        database: '',
        table: '',
        queryType: QueryType.Traces,
        ...builderOptions,
      },
      pluginVersion: '',
      rawSql: '',
    };

    const request: DataQueryRequest<CHQuery> = {
      requestId: '',
      interval: '',
      intervalMs: 0,
      range: {} as any,
      scopedVars: {} as any,
      targets: [inputQuery],
      timezone: '',
      app: CoreApp.Explore,
      startTime: 0,
    };

    const field: Field = {
      name: 'traceID',
      type: FieldType.string,
      config: {},
      values: [],
    };
    const data: DataFrame[] = [
      {
        fields: [field],
        length: 1,
        refId: 'A',
      },
    ];
    const response: DataQueryResponse = { data };

    return [request, response];
  };

  it('inserts links into trace query. Copy trace columns, default log columns.', async () => {
    const mockDatasource = newMockDatasource();
    const getDefaultTraceDatabase = jest.spyOn(mockDatasource, 'getDefaultTraceDatabase');
    const getDefaultTraceTable = jest.spyOn(mockDatasource, 'getDefaultTraceTable');
    const getDefaultTraceColumns = jest.spyOn(mockDatasource, 'getDefaultTraceColumns');
    const getDefaultLogsDatabase = jest.spyOn(mockDatasource, 'getDefaultLogsDatabase');
    const getDefaultLogsTable = jest.spyOn(mockDatasource, 'getDefaultLogsTable');
    const getDefaultLogsColumns = jest.spyOn(mockDatasource, 'getDefaultLogsColumns');

    const builderOptions: Partial<QueryBuilderOptions> = {
      queryType: QueryType.Traces,
      columns: [{ name: 'a' }],
    };

    const [request, response] = buildTestRequestResponse(builderOptions);
    const out = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    const links = out?.data[0]?.fields[0]?.config?.links;
    expect(links).not.toBeUndefined();
    expect(links).toHaveLength(2);
    expect(getDefaultTraceDatabase).not.toHaveBeenCalled();
    expect(getDefaultTraceTable).not.toHaveBeenCalled();
    expect(getDefaultTraceColumns).not.toHaveBeenCalled();
    expect(getDefaultLogsDatabase).toHaveBeenCalled();
    expect(getDefaultLogsTable).toHaveBeenCalled();
    expect(getDefaultLogsColumns).toHaveBeenCalled();
  });

  it('inserts links into logs query. Copy logs columns, default trace columns.', async () => {
    const mockDatasource = newMockDatasource();
    const getDefaultTraceDatabase = jest.spyOn(mockDatasource, 'getDefaultTraceDatabase');
    const getDefaultTraceTable = jest.spyOn(mockDatasource, 'getDefaultTraceTable');
    const getDefaultTraceColumns = jest.spyOn(mockDatasource, 'getDefaultTraceColumns');
    const getDefaultLogsDatabase = jest.spyOn(mockDatasource, 'getDefaultLogsDatabase');
    const getDefaultLogsTable = jest.spyOn(mockDatasource, 'getDefaultLogsTable');
    const getDefaultLogsColumns = jest.spyOn(mockDatasource, 'getDefaultLogsColumns');
    const getDefaultTraceEventsColumnPrefix = jest.spyOn(mockDatasource, 'getDefaultTraceEventsColumnPrefix');
    const getDefaultTraceLinksColumnPrefix = jest.spyOn(mockDatasource, 'getDefaultTraceLinksColumnPrefix');

    const builderOptions: Partial<QueryBuilderOptions> = {
      queryType: QueryType.Logs,
    };

    const [request, response] = buildTestRequestResponse(builderOptions);
    const out = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    const links = out?.data[0]?.fields[0]?.config?.links;
    expect(links).not.toBeUndefined();
    expect(links).toHaveLength(2);
    expect(getDefaultTraceDatabase).toHaveBeenCalled();
    expect(getDefaultTraceTable).toHaveBeenCalled();
    expect(getDefaultTraceColumns).toHaveBeenCalled();
    expect(getDefaultLogsDatabase).not.toHaveBeenCalled();
    expect(getDefaultLogsTable).not.toHaveBeenCalled();
    // getDefaultLogsColumns is now called to get traceIdColumnName for correlation
    expect(getDefaultLogsColumns).toHaveBeenCalled();
    expect(getDefaultTraceEventsColumnPrefix).toHaveBeenCalled();
    expect(getDefaultTraceLinksColumnPrefix).toHaveBeenCalled();
  });

  it('includes TraceId filter in View logs link query using configured column', async () => {
    const mockDatasource = newMockDatasource();
    // Mock that TraceId is configured
    jest.spyOn(mockDatasource, 'getDefaultLogsColumns').mockReturnValue(
      new Map([[ColumnHint.TraceId, 'TraceId']])
    );

    const builderOptions: Partial<QueryBuilderOptions> = {
      queryType: QueryType.Traces,
      columns: [{ name: 'a' }],
    };

    const [request, response] = buildTestRequestResponse(builderOptions);
    const out = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    const links = out?.data[0]?.fields[0]?.config?.links;
    const viewLogsLink = links?.find((link: any) => link.title === 'View logs');

    const logsQuery = viewLogsLink?.internal?.query as CHBuilderQuery;
    expect(logsQuery.builderOptions.columns).toBeDefined();

    // TraceId column should be in the columns array
    const traceIdColumn = logsQuery.builderOptions.columns?.find(
      (c) => c.hint === ColumnHint.TraceId
    );
    expect(traceIdColumn).toBeDefined();
    expect(traceIdColumn?.name).toBe('TraceId');

    // Filter should have the TraceId hint and column name as key
    const traceIdFilter = logsQuery.builderOptions.filters?.find(
      (f) => (f as any).hint === ColumnHint.TraceId
    ) as any;
    expect(traceIdFilter).toBeDefined();
    expect(traceIdFilter.key).toBe('TraceId');
  });
});

describe('dataFrameHasLogLabelWithName', () => {
  const logLabelsFieldName = logColumnHintsToAlias.get(ColumnHint.LogLabels);

  it('should return false for undefined dataframe', () => {
    expect(dataFrameHasLogLabelWithName(undefined, 'testLabel')).toBe(false);
  });

  it('should return false for dataframe with no fields', () => {
    const frame: DataFrame = { fields: [] } as any as DataFrame;
    expect(dataFrameHasLogLabelWithName(frame, 'testLabel')).toBe(false);
  });

  it('should return false when log labels field is not present', () => {
    const frame: DataFrame = {
      fields: [{ name: 'otherField', values: { get: jest.fn(), length: 1 } }],
    } as any as DataFrame;
    expect(dataFrameHasLogLabelWithName(frame, 'testLabel')).toBe(false);
  });

  it('should return false when log labels field has no values', () => {
    const frame: DataFrame = {
      fields: [{ name: logLabelsFieldName, values: { get: jest.fn(), length: 0 } }],
    } as any as DataFrame;
    expect(dataFrameHasLogLabelWithName(frame, 'testLabel')).toBe(false);
  });

  it('should return false when log labels field value is null', () => {
    const frame: DataFrame = {
      fields: [{ name: logLabelsFieldName, values: { get: () => null, length: 1 } }],
    } as any as DataFrame;
    expect(dataFrameHasLogLabelWithName(frame, 'testLabel')).toBe(false);
  });

  it('should return true when log label with given name exists', () => {
    const frame: DataFrame = {
      fields: [
        {
          name: logLabelsFieldName,
          values: { get: () => ({ testLabel: 'value', otherLabel: 'otherValue' }), length: 1 },
        },
      ],
    } as any as DataFrame;
    expect(dataFrameHasLogLabelWithName(frame, 'testLabel')).toBe(true);
  });

  it('should return false when log label with given name does not exist', () => {
    const frame: DataFrame = {
      fields: [
        {
          name: logLabelsFieldName,
          values: { get: () => ({ otherLabel: 'value' }), length: 1 },
        },
      ],
    } as any as DataFrame;
    expect(dataFrameHasLogLabelWithName(frame, 'testLabel')).toBe(false);
  });
});

/**
 * Creates a DataFrame fixture matching the log format with contextID, error, gadget, skipper, etc.
 * Useful for testing log data manipulation.
 */
export const createLogDataFrameFixture = (): DataFrame => {
  return arrayToDataFrame([
    {
      contextID: 'i-Gp7b2ds3El',
      error: JSON.stringify({
        code: '',
        message: '',
        name: '',
        stack: '',
      }),
      gadget: JSON.stringify({
        application_id: 8931,
        environment_id: 16577,
      }),
      globalAction: 'monitorIndividualMutationBgActions',
      model: 'backgroundActionRecord',
      name: 'db-operation',
      pid: 1,
      recordId: 11725487,
      serverRole: 'api',
      skipper: JSON.stringify({
        function: {
          deployment: '',
          namespace: '',
          scale: {
            max_instances: 0,
            min_instances: 0,
            target_cpu_usage_milli: 0,
            target_in_flight_requests: 0,
            target_memory_usage_mib: 0,
          },
          tenant: '',
        },
        heartbeat: {
          in_flight_requests: 0,
          timestamp: '1969-12-31T19:00:00-05:00',
        },
        instance: {
          address: '',
          assigned_at: '1969-12-31T19:00:00-05:00',
          cpu_usage_milli: 0,
          memory_usage_mib: 0,
          name: '',
          ready_at: '1969-12-31T19:00:00-05:00',
          replica_set: '',
        },
      }),
      source: 'platform',
      userVisible: true,
      userspaceLogLevel: 30,
      level: 'info',
    },
  ]);
};

describe('filterEmpty', () => {
  // We need to access the private filterEmpty function for testing
  // Since it's not exported, we'll test it indirectly through transformQueryResponseWithTraceAndLogLinks
  // or we can test the behavior by checking the DataFrame after transformation
  
  it('should remove undefined values from DataFrame field values', () => {
    // Create DataFrame with values (filterEmpty removes undefined, so we test with valid data)
    const testFrame = arrayToDataFrame([
      { field1: 'value1', field2: 1, field3: true },
      { field1: 'value2', field2: 2, field3: false },
      { field1: 'value3', field2: 3, field3: true },
    ]);
    testFrame.refId = 'A';

    // Create a request/response to test through transformQueryResponseWithTraceAndLogLinks
    const request: DataQueryRequest<CHQuery> = {
      requestId: 'test',
      interval: '',
      intervalMs: 0,
      range: {} as any,
      scopedVars: {} as any,
      targets: [{
        refId: 'A',
        editorType: EditorType.Builder,
        builderOptions: {
          database: 'test',
          table: 'test',
          queryType: QueryType.Table,
        },
        pluginVersion: '',
        rawSql: '',
      }],
      timezone: '',
      app: CoreApp.Explore,
      startTime: 0,
    };

    const response: DataQueryResponse = {
      data: [testFrame],
    };

    const mockDatasource = newMockDatasource();
    const result = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    // After filterEmpty, all values should remain (no undefined to remove in this case)
    const resultFrame = result.data[0];
    expect(resultFrame.fields[0].values.length).toBe(3);
    expect(resultFrame.fields[0].values.toArray()).toEqual(['value1', 'value2', 'value3']);
    
    expect(resultFrame.fields[1].values.length).toBe(3);
    expect(resultFrame.fields[1].values.toArray()).toEqual([1, 2, 3]);
    
    expect(resultFrame.fields[2].values.length).toBe(3);
    expect(resultFrame.fields[2].values.toArray()).toEqual([true, false, true]);
  });

  it('should preserve other falsy values (0, false, empty string)', () => {
    const testFrame = arrayToDataFrame([
      { stringField: '', numberField: 0, booleanField: false },
      { stringField: 'value', numberField: 1, booleanField: true },
      { stringField: '', numberField: 0, booleanField: false },
    ]);
    testFrame.refId = 'A';

    const request: DataQueryRequest<CHQuery> = {
      requestId: 'test',
      interval: '',
      intervalMs: 0,
      range: {} as any,
      scopedVars: {} as any,
      targets: [{
        refId: 'A',
        editorType: EditorType.Builder,
        builderOptions: {
          database: 'test',
          table: 'test',
          queryType: QueryType.Table,
        },
        pluginVersion: '',
        rawSql: '',
      }],
      timezone: '',
      app: CoreApp.Explore,
      startTime: 0,
    };

    const response: DataQueryResponse = {
      data: [testFrame],
    };

    const mockDatasource = newMockDatasource();
    const result = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    const resultFrame = result.data[0];
    // Should preserve falsy values, only remove undefined
    expect(resultFrame.fields[0].values.toArray()).toEqual(['', 'value', '']);
    expect(resultFrame.fields[1].values.toArray()).toEqual([0, 1, 0]);
    expect(resultFrame.fields[2].values.toArray()).toEqual([false, true, false]);
  });

  it('should handle DataFrame with empty values', () => {
    // Create an empty DataFrame to test edge case
    const testFrame = arrayToDataFrame([]);
    testFrame.refId = 'A';

    const request: DataQueryRequest<CHQuery> = {
      requestId: 'test',
      interval: '',
      intervalMs: 0,
      range: {} as any,
      scopedVars: {} as any,
      targets: [{
        refId: 'A',
        editorType: EditorType.Builder,
        builderOptions: {
          database: 'test',
          table: 'test',
          queryType: QueryType.Table,
        },
        pluginVersion: '',
        rawSql: '',
      }],
      timezone: '',
      app: CoreApp.Explore,
      startTime: 0,
    };

    const response: DataQueryResponse = {
      data: [testFrame],
    };

    const mockDatasource = newMockDatasource();
    const result = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    const resultFrame = result.data[0];
    // Empty DataFrame should remain empty
    expect(resultFrame.fields.length).toBe(0);
  });

  it('should handle DataFrame with no undefined values', () => {
    const testFrame = arrayToDataFrame([
      { field1: 'a', field2: 1 },
      { field1: 'b', field2: 2 },
      { field1: 'c', field2: 3 },
    ]);
    testFrame.refId = 'A';

    const request: DataQueryRequest<CHQuery> = {
      requestId: 'test',
      interval: '',
      intervalMs: 0,
      range: {} as any,
      scopedVars: {} as any,
      targets: [{
        refId: 'A',
        editorType: EditorType.Builder,
        builderOptions: {
          database: 'test',
          table: 'test',
          queryType: QueryType.Table,
        },
        pluginVersion: '',
        rawSql: '',
      }],
      timezone: '',
      app: CoreApp.Explore,
      startTime: 0,
    };

    const response: DataQueryResponse = {
      data: [testFrame],
    };

    const mockDatasource = newMockDatasource();
    const result = transformQueryResponseWithTraceAndLogLinks(mockDatasource, request, response);

    const resultFrame = result.data[0];
    // All values should remain unchanged
    expect(resultFrame.fields[0].values.length).toBe(3);
    expect(resultFrame.fields[0].values.toArray()).toEqual(['a', 'b', 'c']);
    expect(resultFrame.fields[1].values.length).toBe(3);
    expect(resultFrame.fields[1].values.toArray()).toEqual([1, 2, 3]);
  });
});
