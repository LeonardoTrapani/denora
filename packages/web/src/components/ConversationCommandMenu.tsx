import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@denora/ui/components/command";
import { IconMessage, IconPencilPlus } from "@tabler/icons-react";
import { useNavigate } from "@tanstack/react-router";

import type { ConversationSummary } from "../chat/atoms.ts";

export interface ConversationCommandMenuProps {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  readonly conversations: ReadonlyArray<ConversationSummary>;
  readonly onNewConversation: () => void;
}

export function ConversationCommandMenu({
  open,
  onOpenChange,
  conversations,
  onNewConversation,
}: ConversationCommandMenuProps) {
  const navigate = useNavigate();

  const startNew = () => {
    onOpenChange(false);
    onNewConversation();
  };

  const goTo = (conversationId: string) => {
    onOpenChange(false);
    void navigate({
      to: "/app/conversations/$conversationId",
      params: { conversationId },
    });
  };

  return (
    <CommandDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Search conversations"
      description="Jump to a conversation or start a new one"
    >
      <Command>
        <CommandInput placeholder="Search conversations…" />
        <CommandList>
          <CommandEmpty>No conversations found.</CommandEmpty>
          <CommandGroup heading="Actions">
            <CommandItem value="new chat conversation" onSelect={startNew}>
              <IconPencilPlus />
              New chat
            </CommandItem>
          </CommandGroup>
          {conversations.length > 0 ? (
            <CommandGroup heading="Conversations">
              {conversations.map((conversation) => (
                <CommandItem
                  key={conversation.id}
                  value={`${conversation.title ?? "Untitled conversation"} ${conversation.id}`}
                  onSelect={() => goTo(conversation.id)}
                >
                  <IconMessage />
                  <span className="truncate">{conversation.title ?? "Untitled conversation"}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          ) : null}
        </CommandList>
      </Command>
    </CommandDialog>
  );
}
