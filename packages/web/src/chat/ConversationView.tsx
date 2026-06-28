import {
  Attachment,
  AttachmentAction,
  AttachmentActions,
  AttachmentContent,
  AttachmentDescription,
  AttachmentGroup,
  AttachmentMedia,
  AttachmentTitle,
} from "@denora/ui/components/attachment";
import { Bubble, BubbleContent } from "@denora/ui/components/bubble";
import { Marker, MarkerContent } from "@denora/ui/components/marker";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupTextarea,
} from "@denora/ui/components/input-group";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@denora/ui/components/select";
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from "@denora/ui/components/message-scroller";
import { Reasoning, ReasoningContent, ReasoningTrigger } from "@denora/ui/components/reasoning";
import { Response } from "@denora/ui/components/response";
import { SidebarTrigger } from "@denora/ui/components/sidebar";
import { Spinner } from "@denora/ui/components/spinner";
import {
  Tool,
  ToolContent,
  ToolHeader,
  ToolSection,
  type ToolStatus,
} from "@denora/ui/components/tool";
import {
  IconAlertTriangle,
  IconArrowUp,
  IconBrain,
  IconMicrophone,
  IconPhoto,
  IconX,
} from "@tabler/icons-react";
import { useAtomSet } from "@effect/atom-react";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type {
  AiModelCatalogItem,
  AiModelsResponse,
  AiThinkingLevelItem,
} from "@denora/server/http/Api";

import { loadConversationsAtom } from "./atoms.ts";
import { Api } from "../lib/api.ts";
import { LoadingStates } from "./LoadingStates.tsx";
import type { ChatMessage, ChatMessagePart, ChatStatus } from "./types.ts";
import type { UseConversationChatResult } from "./useConversationChat.ts";

export interface Props {
  readonly chat: UseConversationChatResult;
  readonly title?: string | null | undefined;
  readonly displayName?: string | null | undefined;
}

interface SelectedImage {
  readonly id: string;
  readonly name: string;
  readonly mimeType: string;
  readonly data: string;
  readonly url: string;
}

export function View({ chat, title, displayName }: Props) {
  const catalog = useAiCatalog();
  const defaultModelId = catalog?.defaultModelId ?? "openrouter/openai/gpt-5.5";
  const defaultThinkingLevel = catalog?.defaultThinkingLevel ?? "medium";
  const [composerText, setComposerText] = useState("");
  const [images, setImages] = useState<ReadonlyArray<SelectedImage>>([]);
  const [modelId, setModelId] = useState<string>(defaultModelId);
  const [thinkingLevel, setThinkingLevel] = useState<string>(defaultThinkingLevel);
  const [sendPending, setSendPending] = useState(false);
  const [sendError, setSendError] = useState<Error | undefined>(undefined);
  const refreshConversations = useAtomSet(loadConversationsAtom, { mode: "promise" });
  const showHistorySkeleton = !chat.historyReady && chat.messages.length === 0;
  const isEmptyChat = chat.historyReady && chat.messages.length === 0;
  const error = sendError ?? chat.error;
  const model = useMemo(() => findModel(catalog, modelId), [catalog, modelId]);
  const thinkingLevels = useMemo(
    () => thinkingLevelItemsForModel(catalog?.thinkingLevels ?? fallbackThinkingLevels, model),
    [catalog, model],
  );
  const canThink = thinkingLevels.some((level) => level.id !== "off");
  const selectedThinkingLevel = thinkingLevels.some((level) => level.id === thinkingLevel)
    ? thinkingLevel
    : (thinkingLevels.find((level) => level.default)?.id ?? thinkingLevels[0]?.id ?? "off");
  const effectiveThinkingLevel = canThink ? selectedThinkingLevel : "off";
  const canSend = (composerText.trim().length > 0 || images.length > 0) && !sendPending;

  useEffect(() => setModelId(defaultModelId), [defaultModelId]);
  useEffect(() => setThinkingLevel(defaultThinkingLevel), [defaultThinkingLevel]);

  const send = (event: FormEvent) => {
    event.preventDefault();
    const message = composerText.trim();
    if ((message.length === 0 && images.length === 0) || sendPending) return;
    const submittedImages = images;
    setComposerText("");
    setImages([]);
    setSendPending(true);
    setSendError(undefined);
    void chat
      .sendMessage(message, {
        images: submittedImages.map(({ data, mimeType }) => ({ data, mimeType })),
        modelId,
        thinkingLevel: effectiveThinkingLevel,
      })
      .then(() => {
        void refreshConversations();
      })
      .catch((cause: unknown) => {
        setComposerText(message);
        setImages(submittedImages);
        setSendError(toError(cause));
      })
      .finally(() => setSendPending(false));
  };

  const composer = (
    <Composer
      canSend={canSend}
      composerText={composerText}
      images={images}
      model={model}
      modelId={modelId}
      models={catalogModels(catalog)}
      sendPending={sendPending}
      thinkingLevel={effectiveThinkingLevel}
      thinkingLevels={thinkingLevels}
      variant={isEmptyChat ? "hero" : "dock"}
      onImagesChange={setImages}
      onModelIdChange={setModelId}
      onSend={send}
      onTextChange={setComposerText}
      onThinkingLevelChange={setThinkingLevel}
    />
  );

  return (
    <div className="flex h-svh min-h-0 flex-col overflow-hidden bg-background">
      <header className="flex h-14 shrink-0 items-center gap-2 border-b border-border/40 bg-background/75 px-4 backdrop-blur-xl">
        <SidebarTrigger />
        <h1 className="truncate text-sm font-medium text-foreground/80">{title ?? "New chat"}</h1>
        <ChatStatus status={chat.status} historyReady={chat.historyReady} />
      </header>

      <section className="flex min-h-0 flex-1 flex-col overflow-hidden">
        {isEmptyChat ? (
          <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-24">
            <div className="w-full max-w-2xl space-y-8">
              <div className="space-y-3 text-center">
                <p className="text-sm font-medium text-muted-foreground">Denora</p>
                <h2 className="text-4xl font-medium tracking-tight text-balance sm:text-5xl">
                  {greeting()}, {firstName(displayName) ?? "there"}
                </h2>
                <p className="text-sm text-muted-foreground">What should we work on?</p>
              </div>
              <ErrorBanner error={error} />
              {composer}
            </div>
          </div>
        ) : (
          <>
            <MessageScrollerProvider
              autoScroll
              defaultScrollPosition="last-anchor"
              scrollPreviousItemPeek={64}
            >
              <MessageScroller className="flex-1">
                <MessageScrollerViewport>
                  <MessageScrollerContent
                    aria-busy={chat.status === "streaming" || !chat.historyReady}
                    className="mx-auto w-full max-w-3xl gap-8 px-4 py-6"
                  >
                    {showHistorySkeleton ? (
                      <MessageScrollerItem messageId="loading-history">
                        <LoadingStates.ConversationHistorySkeleton />
                      </MessageScrollerItem>
                    ) : (
                      chat.messages.map((message) => (
                        <MessageScrollerItem
                          key={message.id}
                          messageId={message.id}
                          scrollAnchor={message.role === "user"}
                        >
                          <ChatMessageView message={message} />
                        </MessageScrollerItem>
                      ))
                    )}
                  </MessageScrollerContent>
                </MessageScrollerViewport>
                <MessageScrollerButton />
              </MessageScroller>
            </MessageScrollerProvider>

            <div className="shrink-0 px-4 pb-4">
              <ErrorBanner error={error} />
              <div className="mx-auto w-full max-w-3xl">{composer}</div>
            </div>
          </>
        )}
      </section>
    </div>
  );
}

function Composer({
  canSend,
  composerText,
  images,
  model,
  modelId,
  models,
  sendPending,
  thinkingLevel,
  thinkingLevels,
  variant,
  onImagesChange,
  onModelIdChange,
  onSend,
  onTextChange,
  onThinkingLevelChange,
}: {
  readonly canSend: boolean;
  readonly composerText: string;
  readonly images: ReadonlyArray<SelectedImage>;
  readonly model: AiModelCatalogItem | undefined;
  readonly modelId: string;
  readonly models: ReadonlyArray<AiModelCatalogItem>;
  readonly sendPending: boolean;
  readonly thinkingLevel: string;
  readonly thinkingLevels: ReadonlyArray<AiThinkingLevelItem>;
  readonly variant: "hero" | "dock";
  readonly onImagesChange: (images: ReadonlyArray<SelectedImage>) => void;
  readonly onModelIdChange: (modelId: string) => void;
  readonly onSend: (event: FormEvent) => void;
  readonly onTextChange: (text: string) => void;
  readonly onThinkingLevelChange: (thinkingLevel: string) => void;
}) {
  const fileInput = useRef<HTMLInputElement>(null);
  const canThink = model?.capabilities.reasoning ?? true;

  return (
    <form
      className={
        variant === "hero"
          ? "mx-auto w-full rounded-[2rem] border border-border/70 bg-background/90 p-2 shadow-xl shadow-foreground/5 backdrop-blur-xl"
          : "rounded-[2rem] border border-border/50 bg-background/70 p-2 shadow-sm shadow-foreground/5 backdrop-blur-xl"
      }
      onSubmit={onSend}
    >
      {images.length > 0 ? (
        <AttachmentGroup className="px-1 pb-2">
          {images.map((image) => (
            <Attachment key={image.id} orientation="vertical" size="sm">
              <AttachmentMedia variant="image">
                <img alt={image.name} src={image.url} />
              </AttachmentMedia>
              <AttachmentContent>
                <AttachmentTitle>{image.name}</AttachmentTitle>
                <AttachmentDescription>{image.mimeType}</AttachmentDescription>
              </AttachmentContent>
              <AttachmentActions>
                <AttachmentAction
                  aria-label={`Remove ${image.name}`}
                  type="button"
                  onClick={() => onImagesChange(images.filter((item) => item.id !== image.id))}
                >
                  <IconX />
                </AttachmentAction>
              </AttachmentActions>
            </Attachment>
          ))}
        </AttachmentGroup>
      ) : null}

      <InputGroup className="border-0 bg-transparent shadow-none">
        <InputGroupTextarea
          className="min-h-14 px-2 text-base"
          disabled={sendPending}
          placeholder="Message Denora…"
          rows={variant === "hero" ? 2 : 1}
          value={composerText}
          onChange={(event) => onTextChange(event.currentTarget.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              event.currentTarget.form?.requestSubmit();
            }
          }}
        />
        <InputGroupAddon align="block-end" className="flex-wrap gap-2 px-1 pb-1">
          <input
            accept="image/*"
            className="sr-only"
            multiple
            ref={fileInput}
            type="file"
            onChange={(event) => {
              void readImages(event.currentTarget.files).then((next) =>
                onImagesChange([...images, ...next]),
              );
              event.currentTarget.value = "";
            }}
          />
          <InputGroupButton aria-label="Attach image" onClick={() => fileInput.current?.click()}>
            <IconPhoto />
          </InputGroupButton>
          <InputGroupButton aria-label="Record audio" title="Audio coming soon">
            <IconMicrophone />
          </InputGroupButton>

          <div className="flex min-w-0 flex-1 flex-wrap items-center gap-2">
            <ModelSelect models={models} value={modelId} onValueChange={onModelIdChange} />
            <ThinkingSelect
              disabled={!canThink}
              levels={thinkingLevels}
              value={thinkingLevel}
              onValueChange={onThinkingLevelChange}
            />
          </div>

          <InputGroupButton
            aria-label="Send message"
            className="ml-auto rounded-full"
            disabled={!canSend}
            size="icon-sm"
            type="submit"
            variant="default"
          >
            {sendPending ? <Spinner /> : <IconArrowUp />}
          </InputGroupButton>
        </InputGroupAddon>
      </InputGroup>
    </form>
  );
}

function ModelSelect({
  models,
  value,
  onValueChange,
}: {
  readonly models: ReadonlyArray<AiModelCatalogItem>;
  readonly value: string;
  readonly onValueChange: (value: string) => void;
}) {
  return (
    <Select value={value} onValueChange={(next) => next !== null && onValueChange(next)}>
      <SelectTrigger className="max-w-48 border-0 bg-muted/60" size="sm">
        <SelectValue>{modelName(models, value)}</SelectValue>
      </SelectTrigger>
      <SelectContent align="start" alignItemWithTrigger={false} className="min-w-72">
        <SelectGroup>
          <SelectLabel>Model</SelectLabel>
          {models.map((model) => (
            <SelectItem key={model.id} value={model.id}>
              <span className="flex flex-col items-start gap-0.5">
                <span>{model.name}</span>
                <span className="text-xs font-normal text-muted-foreground">
                  {model.displayProvider.name}
                  {model.capabilities.reasoning ? " · Thinking" : ""}
                  {model.inputModalities.includes("image") ? " · Images" : ""}
                </span>
              </span>
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}

function ThinkingSelect({
  disabled,
  levels,
  value,
  onValueChange,
}: {
  readonly disabled: boolean;
  readonly levels: ReadonlyArray<AiThinkingLevelItem>;
  readonly value: string;
  readonly onValueChange: (value: string) => void;
}) {
  return (
    <Select
      value={disabled ? "off" : value}
      onValueChange={(next) => next !== null && onValueChange(next)}
    >
      <SelectTrigger className="border-0 bg-muted/60" disabled={disabled} size="sm">
        <IconBrain className="size-4" />
        <SelectValue>{thinkingLevelName(levels, disabled ? "off" : value)}</SelectValue>
      </SelectTrigger>
      <SelectContent align="start" alignItemWithTrigger={false} className="min-w-60">
        <SelectGroup>
          <SelectLabel>Thinking level</SelectLabel>
          {levels.map((level) => (
            <SelectItem key={level.id} value={level.id}>
              <span className="flex flex-col items-start gap-0.5">
                <span>{level.name}</span>
                <span className="text-xs font-normal text-muted-foreground">
                  {level.description}
                </span>
              </span>
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}

function ErrorBanner({ error }: { readonly error: Error | undefined }) {
  return error === undefined ? null : (
    <div className="mx-auto mb-2 flex w-full max-w-3xl items-center gap-2 text-sm text-destructive">
      <IconAlertTriangle className="size-4 shrink-0" />
      <span>{error.message}</span>
    </div>
  );
}

function useAiCatalog(): AiModelsResponse | undefined {
  const [catalog, setCatalog] = useState<AiModelsResponse | undefined>();

  useEffect(() => {
    let cancelled = false;
    void Api.runApi(
      Api.apiEffect((client) => client.listAiModels()),
      {
        span: "chat.listAiModels",
      },
    )
      .then((response) => {
        if (!cancelled) setCatalog(response);
      })
      .catch(() => {
        if (!cancelled) setCatalog(fallbackCatalog);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return catalog ?? fallbackCatalog;
}

async function readImages(files: FileList | null): Promise<ReadonlyArray<SelectedImage>> {
  if (files === null) return [];
  return Promise.all(
    [...files]
      .filter((file) => file.type.startsWith("image/"))
      .map(async (file) => {
        const dataUrl = await readDataUrl(file);
        const data = dataUrl.slice(dataUrl.indexOf(",") + 1);
        return {
          id: crypto.randomUUID(),
          name: file.name,
          mimeType: file.type,
          data,
          url: dataUrl,
        };
      }),
  );
}

function readDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.addEventListener("load", () => resolve(String(reader.result ?? "")), { once: true });
    reader.addEventListener("error", () => reject(reader.error ?? new Error("Image read failed")), {
      once: true,
    });
    reader.readAsDataURL(file);
  });
}

function catalogModels(catalog: AiModelsResponse | undefined): ReadonlyArray<AiModelCatalogItem> {
  return catalog?.providers.flatMap((provider) => provider.models) ?? fallbackCatalogModels;
}

function findModel(
  catalog: AiModelsResponse | undefined,
  modelId: string,
): AiModelCatalogItem | undefined {
  return catalogModels(catalog).find((candidate) => candidate.id === modelId);
}

function modelName(models: ReadonlyArray<AiModelCatalogItem>, modelId: string): string {
  return models.find((model) => model.id === modelId)?.name ?? modelId;
}

function thinkingLevelName(levels: ReadonlyArray<AiThinkingLevelItem>, value: string): string {
  return levels.find((level) => level.id === value)?.name ?? value;
}

function thinkingLevelItemsForModel(
  levels: ReadonlyArray<AiThinkingLevelItem>,
  model: AiModelCatalogItem | undefined,
): ReadonlyArray<AiThinkingLevelItem> {
  const supported = model?.capabilities.thinkingLevels;
  if (supported === undefined) return levels;
  const items = levels.filter((level) => supported.includes(level.id));
  return items.length === 0 ? levels.filter((level) => level.id === "off") : items;
}

function greeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return "Morning";
  if (hour < 18) return "Afternoon";
  return "Evening";
}

function firstName(name: string | null | undefined): string | undefined {
  const trimmed = name?.trim();
  if (!trimmed) return undefined;
  return trimmed.split(/\s+/)[0];
}

const fallbackThinkingLevels: ReadonlyArray<AiThinkingLevelItem> = [
  { id: "off", name: "Off", description: "No explicit reasoning budget.", default: false },
  {
    id: "minimal",
    name: "Minimal",
    description: "Fastest reasoning-capable responses.",
    default: false,
  },
  { id: "low", name: "Low", description: "Light reasoning for simple tasks.", default: false },
  {
    id: "medium",
    name: "Medium",
    description: "Balanced reasoning for everyday work.",
    default: true,
  },
  { id: "high", name: "High", description: "More reasoning for hard tasks.", default: false },
  {
    id: "xhigh",
    name: "Extra high",
    description: "Maximum reasoning for hard tasks.",
    default: false,
  },
];

const fallbackCatalogModels: ReadonlyArray<AiModelCatalogItem> = [
  {
    id: "openrouter/openai/gpt-5.5",
    name: "GPT-5.5",
    displayProvider: { id: "openrouter", name: "OpenRouter" },
    family: "default",
    default: true,
    api: "openai-completions",
    capabilities: {
      reasoning: true,
      thinkingLevels: fallbackThinkingLevels.map((level) => level.id),
      reasoningMode: "openai-compatible",
      tools: true,
    },
    inputModalities: ["text", "image"],
    outputModalities: ["text"],
    contextWindow: 0,
    maxTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    lifecycle: "stable",
  },
];

const fallbackCatalog: AiModelsResponse = {
  defaultModelId: "openrouter/openai/gpt-5.5",
  defaultThinkingLevel: "medium",
  thinkingLevels: fallbackThinkingLevels,
  providers: [
    {
      id: "openrouter",
      name: "OpenRouter",
      models: fallbackCatalogModels,
    },
  ],
};

function ChatStatus({
  status,
  historyReady,
}: {
  readonly status: ChatStatus;
  readonly historyReady: boolean;
}) {
  const busy = status === "streaming" || status === "submitted" || status === "connecting";
  if (historyReady && !busy) return null;
  return (
    <span className="ml-auto flex items-center gap-1.5 text-xs text-muted-foreground">
      <Spinner className="size-3.5" />
      {historyReady ? statusLabel(status) : "Syncing history…"}
    </span>
  );
}

function ChatMessageView({ message }: { readonly message: ChatMessage }) {
  if (message.role === "system") {
    return (
      <Marker variant="separator">
        <MarkerContent>{plainMessageText(message)}</MarkerContent>
      </Marker>
    );
  }

  if (message.role === "user") {
    return (
      <div className="flex justify-end">
        <Bubble align="end" variant="default">
          <BubbleContent className="whitespace-pre-wrap">{plainMessageText(message)}</BubbleContent>
        </Bubble>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3 text-sm leading-relaxed">
      {message.parts.length === 0 ? (
        <LoadingStates.AssistantTyping />
      ) : (
        message.parts.map((part, index) => <MessagePart key={index} part={part} />)
      )}
    </div>
  );
}

function MessagePart({ part }: { readonly part: ChatMessagePart }) {
  switch (part.type) {
    case "text":
      return <Response>{part.text}</Response>;
    case "reasoning":
      return (
        <Reasoning isStreaming={part.state === "streaming"}>
          <ReasoningTrigger />
          <ReasoningContent>
            <Response>{part.text}</Response>
          </ReasoningContent>
        </Reasoning>
      );
    case "dynamic-tool": {
      const status: ToolStatus =
        part.state === "output-available"
          ? "complete"
          : part.state === "output-error"
            ? "error"
            : "running";
      return (
        <Tool defaultOpen={part.state === "output-error"}>
          <ToolHeader name={part.toolName} status={status} />
          <ToolContent>
            <ToolSection title="Input">
              <Response>{asCodeBlock(part.input)}</Response>
            </ToolSection>
            {part.state === "output-available" ? (
              <ToolSection title="Output">
                <Response>{asCodeBlock(part.output)}</Response>
              </ToolSection>
            ) : null}
            {part.state === "output-error" ? (
              <ToolSection title="Error">
                <p className="text-sm text-destructive">{part.errorText}</p>
              </ToolSection>
            ) : null}
          </ToolContent>
        </Tool>
      );
    }
    case "file":
      return part.mediaType.startsWith("image/") && part.url.startsWith("data:") ? (
        <Attachment orientation="vertical">
          <AttachmentMedia variant="image">
            <img alt="Attached file" src={part.url} />
          </AttachmentMedia>
          <AttachmentContent>
            <AttachmentTitle>Image</AttachmentTitle>
            <AttachmentDescription>{part.mediaType}</AttachmentDescription>
          </AttachmentContent>
        </Attachment>
      ) : (
        <Attachment>
          <AttachmentMedia>□</AttachmentMedia>
          <AttachmentContent>
            <AttachmentTitle>File</AttachmentTitle>
            <AttachmentDescription>{part.mediaType}</AttachmentDescription>
          </AttachmentContent>
        </Attachment>
      );
  }
}

function plainMessageText(message: ChatMessage): string {
  return message.parts
    .map((part) => (part.type === "text" || part.type === "reasoning" ? part.text : ""))
    .join("")
    .trim();
}

function statusLabel(status: ChatStatus): string {
  switch (status) {
    case "idle":
      return "Idle";
    case "connecting":
      return "Connecting";
    case "submitted":
      return "Submitted";
    case "streaming":
      return "Streaming";
    case "error":
      return "Error";
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function asCodeBlock(value: unknown): string {
  const text = typeof value === "string" ? value : formatUnknown(value);
  const language = typeof value === "string" ? "" : "json";
  return `\`\`\`${language}\n${text}\n\`\`\``;
}

function formatUnknown(value: unknown): string {
  if (value === undefined) return "undefined";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    return String(value);
  }
}

export * as ConversationView from "./ConversationView.tsx";
