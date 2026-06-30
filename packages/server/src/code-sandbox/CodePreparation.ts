import { parse } from "@babel/parser";
import { transform } from "sucrase";

const fencedCodeBlock = /```(?:[^\n`]*)?\s*\n([\s\S]*?)```/i;
const functionDeclaration = /^(?:async\s+)?function(?:\s+([a-zA-Z_$][a-zA-Z0-9_$]*))?\s*\(/;
const callableError = "Code must evaluate to a function";

const extractCandidateSource = (code: string): string => {
  const trimmed = code.trim();
  if (trimmed.length === 0) return "";

  const fenced = trimmed.match(fencedCodeBlock)?.[1];
  return (fenced ?? trimmed).trim();
};

const wrapCallableBody = (source: string): string =>
  [
    "const __fn = (",
    source,
    ");",
    `if (typeof __fn !== "function") throw new Error(${JSON.stringify(callableError)});`,
    "return await __fn();",
  ].join("\n");

const wrapNamedFunctionBody = (source: string, name: string): string =>
  [source, `return await ${name}();`].join("\n");

const wrapAnonymousFunctionBody = (source: string): string => `return await (${source})();`;

const sliceNode = (
  source: string,
  node: {
    readonly start?: number | null;
    readonly end?: number | null;
  },
): string => {
  const start = node.start ?? 0;
  const end = node.end ?? source.length;
  return source.slice(start, end);
};

const unwrapExpression = (expression: {
  readonly type: string;
  readonly expression?: unknown;
}): unknown => {
  switch (expression.type) {
    case "ParenthesizedExpression":
    case "TSAsExpression":
    case "TSSatisfiesExpression":
    case "TSTypeAssertion":
    case "TSNonNullExpression":
    case "TSInstantiationExpression":
      return expression.expression
        ? unwrapExpression(expression.expression as { readonly type: string })
        : expression;
    default:
      return expression;
  }
};

interface ExportDefaultDeclarationNode {
  readonly type: string;
  readonly start?: number | null;
  readonly end?: number | null;
  readonly id?: { readonly name?: string | null } | null;
  readonly expression?: unknown;
}

const renderExportDefaultBody = (
  source: string,
  declaration: ExportDefaultDeclarationNode,
): string => {
  if (declaration.type === "FunctionDeclaration") {
    const fnSource = sliceNode(source, declaration);
    const name = declaration.id?.name;
    return name ? wrapNamedFunctionBody(fnSource, name) : wrapAnonymousFunctionBody(fnSource);
  }

  const expression = unwrapExpression(declaration) as { readonly type?: string };
  const expressionSource = sliceNode(source, declaration);

  if (expression.type === "ArrowFunctionExpression" || expression.type === "FunctionExpression") {
    return wrapCallableBody(expressionSource);
  }

  return `return (${expressionSource});`;
};

const renderParsedBody = (source: string): string => {
  const program = parse(source, {
    sourceType: "module",
    allowAwaitOutsideFunction: true,
    allowReturnOutsideFunction: true,
    allowImportExportEverywhere: true,
    plugins: ["typescript"],
  }).program;

  if (program.body.length !== 1) return source;

  const [statement] = program.body;
  if (!statement) return source;

  switch (statement.type) {
    case "ExpressionStatement": {
      const expression = unwrapExpression(
        statement.expression as { readonly type: string; readonly expression?: unknown },
      ) as { readonly type?: string };
      return expression.type === "ArrowFunctionExpression" ||
        expression.type === "FunctionExpression"
        ? wrapCallableBody(source)
        : source;
    }
    case "FunctionDeclaration":
      return statement.id?.name ? wrapNamedFunctionBody(source, statement.id.name) : source;
    case "ExportDefaultDeclaration":
      return renderExportDefaultBody(source, statement.declaration);
    default:
      return source;
  }
};

const renderHeuristicBody = (source: string): string => {
  const withoutDefaultExport = source.replace(/^export\s+default\s+/, "").trim();

  if (
    (withoutDefaultExport.startsWith("async") || withoutDefaultExport.startsWith("(")) &&
    withoutDefaultExport.includes("=>")
  ) {
    return wrapCallableBody(withoutDefaultExport);
  }

  if (functionDeclaration.test(withoutDefaultExport)) {
    const name = withoutDefaultExport.match(functionDeclaration)?.[1];
    return name
      ? wrapNamedFunctionBody(withoutDefaultExport, name)
      : wrapAnonymousFunctionBody(withoutDefaultExport);
  }

  return withoutDefaultExport;
};

export const recoverExecutionBody = (code: string): string => {
  const source = extractCandidateSource(code);
  if (source.length === 0) return "";

  try {
    return renderParsedBody(source);
  } catch {
    return renderHeuristicBody(source);
  }
};

export const stripTypeScript = (code: string): string =>
  transform(code, {
    transforms: ["typescript"],
    disableESTransforms: true,
    keepUnusedImports: true,
  }).code;

export const prepareUserCode = (code: string): string =>
  stripTypeScript(recoverExecutionBody(code));

export * as CodePreparation from "./CodePreparation.ts";
