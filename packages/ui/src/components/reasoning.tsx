import * as React from "react";
import { IconChevronDown, IconSparkles } from "@tabler/icons-react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@denora/ui/components/collapsible";
import { Marker, MarkerContent, MarkerIcon } from "@denora/ui/components/marker";
import { cn } from "@denora/ui/lib/utils";

type ReasoningContextValue = { readonly isStreaming: boolean };

const ReasoningContext = React.createContext<ReasoningContextValue | null>(null);

function useReasoning() {
  const context = React.useContext(ReasoningContext);
  if (!context) {
    throw new Error("Reasoning components must be used within <Reasoning>.");
  }
  return context;
}

function Reasoning({
  className,
  isStreaming = false,
  defaultOpen = false,
  children,
  ...props
}: React.ComponentProps<typeof Collapsible> & { isStreaming?: boolean }) {
  const [open, setOpen] = React.useState(defaultOpen);
  const wasStreaming = React.useRef(isStreaming);

  // Reveal live thoughts while the model reasons, then tuck them away shortly
  // after it moves on to the answer — the reader rarely wants to keep staring
  // at a finished chain of thought, but should still be able to reopen it.
  React.useEffect(() => {
    if (isStreaming) {
      setOpen(true);
    } else if (wasStreaming.current) {
      const timer = setTimeout(() => setOpen(false), 800);
      wasStreaming.current = isStreaming;
      return () => clearTimeout(timer);
    }
    wasStreaming.current = isStreaming;
  }, [isStreaming]);

  return (
    <ReasoningContext.Provider value={{ isStreaming }}>
      <Collapsible
        data-slot="reasoning"
        open={open}
        onOpenChange={setOpen}
        className={cn("flex flex-col gap-2", className)}
        {...props}
      >
        {children}
      </Collapsible>
    </ReasoningContext.Provider>
  );
}

// The trigger is a Marker — the chat-kit's "conversation event" row (its docs
// cite "thinking states" as a use case) — driving a Collapsible so the streamed
// thought text lives in the expandable panel below.
function ReasoningTrigger({
  className,
  children,
  ...props
}: React.ComponentProps<typeof CollapsibleTrigger>) {
  const { isStreaming } = useReasoning();
  return (
    <CollapsibleTrigger
      data-slot="reasoning-trigger"
      {...props}
      render={<Marker className={cn("cursor-pointer hover:text-foreground", className)} />}
    >
      <MarkerIcon>
        <IconSparkles />
      </MarkerIcon>
      <MarkerContent className={cn(isStreaming && "animate-pulse")}>
        {children ?? (isStreaming ? "Thinking…" : "Reasoning")}
      </MarkerContent>
      <IconChevronDown className="ml-auto size-4 shrink-0 transition-transform group-aria-expanded/marker:rotate-180" />
    </CollapsibleTrigger>
  );
}

function ReasoningContent({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <CollapsibleContent
      data-slot="reasoning-content"
      className="h-(--collapsible-panel-height) overflow-hidden transition-[height] duration-200 ease-out data-ending-style:h-0 data-starting-style:h-0"
    >
      <div
        className={cn(
          "border-l-2 pl-4 text-sm text-muted-foreground [&>*:first-child]:mt-0 [&>*:last-child]:mb-0",
          className,
        )}
        {...props}
      />
    </CollapsibleContent>
  );
}

export { Reasoning, ReasoningTrigger, ReasoningContent };
