import * as React from "react";
import { IconChevronDown } from "@tabler/icons-react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@denora/ui/components/collapsible";
import { Marker, MarkerContent } from "@denora/ui/components/marker";
import { cn } from "@denora/ui/lib/utils";

type ReasoningContextValue = {
  readonly isStreaming: boolean;
  readonly duration: number | undefined;
};

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
  const [duration, setDuration] = React.useState<number | undefined>(undefined);
  const startTimeRef = React.useRef<number | null>(null);

  // Measure how long the model spent reasoning so the trigger can report it
  // ("Thought for 12 seconds"). We can only time reasoning we watch stream in;
  // history loads arrive already finished and keep an undefined duration.
  React.useEffect(() => {
    if (isStreaming) {
      if (startTimeRef.current === null) startTimeRef.current = Date.now();
    } else if (startTimeRef.current !== null) {
      setDuration(Math.ceil((Date.now() - startTimeRef.current) / 1000));
      startTimeRef.current = null;
    }
  }, [isStreaming]);

  return (
    <ReasoningContext.Provider value={{ isStreaming, duration }}>
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

// The trigger is a Marker — the chat-kit's "conversation event" row — driving a
// Collapsible, so the streamed thought text lives in the expandable panel below.
function ReasoningTrigger({
  className,
  children,
  ...props
}: React.ComponentProps<typeof CollapsibleTrigger>) {
  const { isStreaming, duration } = useReasoning();
  return (
    <CollapsibleTrigger
      data-slot="reasoning-trigger"
      {...props}
      render={<Marker className={cn("cursor-pointer hover:text-foreground", className)} />}
    >
      <MarkerContent>
        {children ??
          (isStreaming ? (
            <span className="animate-pulse">Thinking…</span>
          ) : duration === undefined ? (
            "Reasoning"
          ) : (
            `Thought for ${formatThoughtDuration(duration)}`
          ))}
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

function formatThoughtDuration(totalSeconds: number): string {
  if (totalSeconds < 60) {
    return `${totalSeconds} second${totalSeconds === 1 ? "" : "s"}`;
  }
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  const minutePart = `${minutes} minute${minutes === 1 ? "" : "s"}`;
  if (seconds === 0) return minutePart;
  return `${minutePart} and ${seconds} second${seconds === 1 ? "" : "s"}`;
}

export { Reasoning, ReasoningTrigger, ReasoningContent };
