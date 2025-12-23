// Auto-generated registry extracted for runtime reuse

export interface McpToolDefinition {
  name: string;
  description: string;
  inputSchema: any;
  method: string;
  pathTemplate: string;
  executionParameters: { name: string; in: string }[];
  requestBodyContentType?: string;
  securityRequirements?: any[];
}

export const toolDefinitionMap: Map<string, McpToolDefinition> = new Map([
  [
    "create-relation",
    {
      name: "create-relation",
      description: "Create a relation between two records",
      inputSchema: {
        type: "object",
        properties: {}
      },
      method: "post",
      pathTemplate: "/associations/relations",
      executionParameters: [
        { name: "Version", in: "header" }
      ],
      requestBodyContentType: "application/json",
      securityRequirements: [
        { bearer: ["associations/relation.write"] }
      ]
    }
  ],
  [
    "get-relations-by-record-id",
    {
      name: "get-relations-by-record-id",
      description: "Get all relations by record id",
      inputSchema: {
        type: "object",
        properties: {}
      },
      method: "get",
      pathTemplate: "/associations/relations/{recordId}",
      executionParameters: [
        { name: "Version", in: "header" },
        { name: "recordId", in: "path" }
      ],
      securityRequirements: [
        { bearer: ["associations/relation.readonly"] }
      ]
    }
  ]
]);

export function listToolsForClient(): McpToolDefinition[] {
  return Array.from(toolDefinitionMap.values());
}
