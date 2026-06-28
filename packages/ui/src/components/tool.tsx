import * as React from "react";
import { IconAlertTriangle, IconChevronDown, IconCircleCheck, IconTool } from "@tabler/icons-react";

import { Badge } from "@denora/ui/components/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@denora/ui/components/collapsible";
import { Marker, MarkerContent, MarkerIcon } from "@denora/ui/components/marker";
import { Spinner } from "@denora/ui/components/spinner";
import { cn } from "@denora/ui/lib/utils";

type ToolStatus = "running" | "complete" | "error";

const TOOL_STATUS: Record<
  ToolStatus,
  {
    readonly label: string;
    readonly variant: React.ComponentProps<typeof Badge>["variant"];
    readonly icon: React.ReactNode;
  }
> = {
  running: { label: "Running", variant: "secondary", icon: <Spinner /> },
  complete: { label: "Done", variant: "secondary", icon: <IconCircleCheck /> },
  error: { label: "Error", variant: "destructive", icon: <IconAlertTriangle /> },
};

function Tool({ className, ...props }: React.ComponentProps<typeof Collapsible>) {
  return (
    <Collapsible data-slot="tool" className={cn("flex flex-col gap-2", className)} {...props} />
  );
}

// Like a reasoning block, a tool call is a Marker-triggered Collapsible: the
// row shows the tool name + live status, and expands to its input/output.
function ToolHeader({
  className,
  name,
  status,
  children,
  ...props
}: React.ComponentProps<typeof CollapsibleTrigger> & {
  name?: React.ReactNode;
  status: ToolStatus;
}) {
  return (
    <CollapsibleTrigger
      data-slot="tool-header"
      {...props}
      render={<Marker className={cn("cursor-pointer hover:text-foreground", className)} />}
    >
      <MarkerIcon>
        <IconTool />
      </MarkerIcon>
      <MarkerContent className="flex-1 truncate font-medium text-foreground">
        {children ?? name}
      </MarkerContent>
      <ToolStatusBadge status={status} />
      <IconChevronDown className="size-4 shrink-0 transition-transform group-aria-expanded/marker:rotate-180" />
    </CollapsibleTrigger>
  );
}

function ToolStatusBadge({ status }: { status: ToolStatus }) {
  const meta = TOOL_STATUS[status];
  return (
    <Badge variant={meta.variant}>
      {meta.icon}
      {meta.label}
    </Badge>
  );
}

function ToolContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <CollapsibleContent
      data-slot="tool-content"
      className="h-(--collapsible-panel-height) overflow-hidden transition-[height] duration-200 ease-out data-ending-style:h-0 data-starting-style:h-0"
    >
      <div className={cn("flex flex-col gap-3 border-l-2 pl-4", className)} {...props} />
    </CollapsibleContent>
  );
}

function ToolSection({
  className,
  title,
  children,
  ...props
}: React.ComponentProps<"div"> & { title: React.ReactNode }) {
  return (
    <div
      data-slot="tool-section"
      className={cn("flex min-w-0 flex-col gap-1.5", className)}
      {...props}
    >
      <span className="text-xs font-medium text-muted-foreground">{title}</span>
      {children}
    </div>
  );
}

export { Tool, ToolHeader, ToolContent, ToolSection, type ToolStatus };
