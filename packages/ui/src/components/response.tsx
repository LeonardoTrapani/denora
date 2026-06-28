import * as React from "react";
import { Streamdown } from "streamdown";

import { cn } from "@denora/ui/lib/utils";

// Streamdown renders streaming markdown safely: it closes unterminated code
// fences/tables mid-stream so partial tokens never flash as raw text. Its own
// element styling ships as Tailwind classes inside streamdown/dist, which is
// why globals.css scans that path with `@source`.
function Response({ className, ...props }: React.ComponentProps<typeof Streamdown>) {
  return (
    <Streamdown
      data-slot="response"
      className={cn("min-w-0 [&>*:first-child]:mt-0 [&>*:last-child]:mb-0", className)}
      {...props}
    />
  );
}

export { Response };
