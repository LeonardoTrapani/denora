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
import {
  Combobox,
  ComboboxContent,
  ComboboxEmpty,
  ComboboxGroup,
  ComboboxItem,
  ComboboxList,
  ComboboxTrigger,
  useComboboxAnchor,
} from "@denora/ui/components/combobox";
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
import { Tooltip, TooltipContent, TooltipTrigger } from "@denora/ui/components/tooltip";
import {
  IconAlertTriangle,
  IconArrowUp,
  IconBrain,
  IconMicrophone,
  IconPhoto,
  IconSearch,
  IconX,
} from "@tabler/icons-react";
import { useAtomSet } from "@effect/atom-react";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type {
  AiModelCatalogItem,
  AiModelProviderGroup,
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
  readonly expectedConversationId?: string | undefined;
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

export function View({ chat, expectedConversationId, title, displayName }: Props) {
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
  const loadingExpectedConversation =
    expectedConversationId !== undefined && chat.conversationId !== expectedConversationId;
  const showHistorySkeleton =
    loadingExpectedConversation || (!chat.historyReady && chat.messages.length === 0);
  const isEmptyChat =
    !loadingExpectedConversation && chat.historyReady && chat.messages.length === 0;
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
  const canSend =
    (composerText.trim().length > 0 || images.length > 0) &&
    !sendPending &&
    !loadingExpectedConversation;
  const displayFirstName = firstName(displayName);
  const heroTitle =
    displayFirstName === undefined
      ? "What should we work on?"
      : `${greeting()}, ${displayFirstName}`;

  useEffect(() => setModelId(defaultModelId), [defaultModelId]);
  useEffect(() => setThinkingLevel(defaultThinkingLevel), [defaultThinkingLevel]);

  const send = (event: FormEvent) => {
    event.preventDefault();
    const message = composerText.trim();
    if (
      (message.length === 0 && images.length === 0) ||
      sendPending ||
      loadingExpectedConversation
    ) {
      return;
    }
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
      providers={catalogProviders(catalog)}
      sendPending={sendPending || loadingExpectedConversation}
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
        <ChatStatus
          status={chat.status}
          historyReady={!loadingExpectedConversation && chat.historyReady}
        />
      </header>

      <section className="flex min-h-0 flex-1 flex-col overflow-hidden">
        {isEmptyChat ? (
          <div className="flex min-h-0 flex-1 items-center justify-center px-4 pb-24">
            <div className="w-full max-w-2xl space-y-8">
              <div className="text-center">
                <h2 className="text-4xl font-medium tracking-tight text-balance sm:text-5xl">
                  {heroTitle}
                </h2>
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
                    aria-busy={
                      chat.status === "streaming" ||
                      !chat.historyReady ||
                      loadingExpectedConversation
                    }
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
  providers,
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
  readonly providers: ReadonlyArray<AiModelProviderGroup>;
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
          placeholder="Message…"
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
            <ModelSelect providers={providers} value={modelId} onValueChange={onModelIdChange} />
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
  providers,
  value,
  onValueChange,
}: {
  readonly providers: ReadonlyArray<AiModelProviderGroup>;
  readonly value: string;
  readonly onValueChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeProviderId, setActiveProviderId] = useState<string | undefined>();
  const anchorRef = useComboboxAnchor();
  const searchInputRef = useRef<HTMLInputElement>(null);
  const models = useMemo(() => providers.flatMap((provider) => provider.models), [providers]);
  const selectedModel = useMemo(() => models.find((model) => model.id === value), [models, value]);
  const selectedProviderId = selectedModel?.displayProvider.id ?? providers[0]?.id;
  const activeProvider =
    providers.find((provider) => provider.id === (activeProviderId ?? selectedProviderId)) ??
    providers[0];
  const hasQuery = query.trim().length > 0;
  const visibleModels = useMemo(
    () =>
      hasQuery
        ? searchModels(models, query, value)
        : sortModelsForPicker(activeProvider?.models ?? [], value),
    [activeProvider, hasQuery, models, query, value],
  );

  useEffect(() => {
    setActiveProviderId((current) =>
      current !== undefined && providers.some((provider) => provider.id === current)
        ? current
        : selectedProviderId,
    );
  }, [providers, selectedProviderId]);

  useEffect(() => {
    if (!open) return;
    const frame = requestAnimationFrame(() => searchInputRef.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, [open]);

  return (
    <Combobox<AiModelCatalogItem>
      value={selectedModel ?? null}
      items={visibleModels}
      inputValue={query}
      open={open}
      autoHighlight
      filter={null}
      isItemEqualToValue={(item, selected) => item.id === selected.id}
      itemToStringLabel={(model) => model.name}
      itemToStringValue={(model) => model.id}
      onInputValueChange={(next) => setQuery(next)}
      onOpenChange={(nextOpen) => {
        setOpen((wasOpen) => {
          if (nextOpen && !wasOpen) {
            setQuery("");
            setActiveProviderId(selectedProviderId);
          }
          return nextOpen;
        });
      }}
      onValueChange={(next) => {
        if (next === null) return;
        setQuery("");
        setActiveProviderId(next.displayProvider.id);
        onValueChange(next.id);
      }}
    >
      <div ref={anchorRef} className="inline-flex min-w-0">
        <ComboboxTrigger
          aria-label="Select model"
          className="flex h-8 max-w-60 min-w-0 items-center gap-1.5 rounded-full bg-muted/60 px-3 text-sm font-medium text-muted-foreground transition-colors hover:bg-muted hover:text-foreground data-pressed:bg-muted"
          type="button"
        >
          <span className="truncate text-foreground/85">{selectedModel?.name ?? value}</span>
        </ComboboxTrigger>
      </div>
      <ComboboxContent
        anchor={anchorRef}
        align="start"
        collisionAvoidance={{ side: "flip", align: "shift", fallbackAxisSide: "none" }}
        initialFocus={searchInputRef}
        positionMethod="fixed"
        className="w-[min(34rem,calc(100vw-2rem))] min-w-[min(34rem,calc(100vw-2rem))] overflow-hidden p-0"
      >
        <div className="border-b border-border/40 bg-muted/20 px-4 py-3">
          <div className="flex items-center gap-2 text-muted-foreground">
            <IconSearch className="size-4 shrink-0" />
            <input
              aria-label="Search models"
              className="h-8 flex-1 border-0 bg-transparent p-0 text-sm outline-none placeholder:text-muted-foreground"
              placeholder="Search models…"
              ref={searchInputRef}
              value={query}
              onChange={(event) => setQuery(event.currentTarget.value)}
              onKeyDown={(event) => {
                if (event.key !== "Escape") event.stopPropagation();
              }}
              onMouseDown={(event) => event.stopPropagation()}
              onPointerDown={(event) => event.stopPropagation()}
            />
          </div>
        </div>
        <div className="grid h-[min(25rem,calc(var(--available-height)-1rem))] grid-cols-[4.25rem_minmax(0,1fr)]">
          <div className="no-scrollbar flex flex-col gap-1 overflow-y-auto border-r border-border/40 bg-muted/20 p-2">
            {providers.map((provider) => (
              <Tooltip key={provider.id}>
                <TooltipTrigger
                  render={
                    <button
                      aria-label={provider.name}
                      className={providerRailClass(provider.id === activeProvider?.id && !hasQuery)}
                      type="button"
                      onMouseDown={(event) => event.preventDefault()}
                      onClick={() => {
                        setQuery("");
                        setActiveProviderId(provider.id);
                      }}
                    />
                  }
                >
                  {providerInitials(provider.name)}
                </TooltipTrigger>
                <TooltipContent side="right">{provider.name}</TooltipContent>
              </Tooltip>
            ))}
          </div>
          <div className="min-w-0 overflow-hidden bg-popover/80">
            <ComboboxEmpty>No models found.</ComboboxEmpty>
            <ComboboxList className="max-h-full p-2">
              <ComboboxGroup>
                {visibleModels.map((model) => (
                  <ModelComboboxItem key={model.id} model={model} />
                ))}
              </ComboboxGroup>
            </ComboboxList>
          </div>
        </div>
      </ComboboxContent>
    </Combobox>
  );
}

function ModelComboboxItem({ model }: { readonly model: AiModelCatalogItem }) {
  return (
    <ComboboxItem value={model} className="items-start rounded-2xl px-3 py-3">
      <span className="grid size-8 shrink-0 place-items-center rounded-xl bg-muted text-xs font-semibold text-muted-foreground">
        {providerInitials(model.displayProvider.name)}
      </span>
      <span className="flex min-w-0 flex-1 flex-col gap-1">
        <span className="flex min-w-0 items-center gap-2">
          <span className="truncate font-semibold text-foreground">{model.name}</span>
          {model.default ? <ModelPill>Default</ModelPill> : null}
          {model.lifecycle === "preview" ? <ModelPill>Preview</ModelPill> : null}
        </span>
        <span className="truncate text-xs font-normal text-muted-foreground">
          {modelSummary(model)}
        </span>
      </span>
    </ComboboxItem>
  );
}

function ModelPill({ children }: { readonly children: string }) {
  return (
    <span className="rounded-full bg-foreground/10 px-1.5 py-0.5 text-[0.65rem] font-medium text-muted-foreground">
      {children}
    </span>
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

function catalogProviders(
  catalog: AiModelsResponse | undefined,
): ReadonlyArray<AiModelProviderGroup> {
  return catalog?.providers ?? fallbackCatalog.providers;
}

function catalogModels(catalog: AiModelsResponse | undefined): ReadonlyArray<AiModelCatalogItem> {
  return catalogProviders(catalog).flatMap((provider) => provider.models);
}

function findModel(
  catalog: AiModelsResponse | undefined,
  modelId: string,
): AiModelCatalogItem | undefined {
  return catalogModels(catalog).find((candidate) => candidate.id === modelId);
}

function searchModels(
  models: ReadonlyArray<AiModelCatalogItem>,
  query: string,
  selectedModelId: string,
): ReadonlyArray<AiModelCatalogItem> {
  const normalizedQuery = normalizeSearch(query);
  if (normalizedQuery.length === 0) return [];
  return models
    .filter((model) => modelMatchesNormalizedQuery(model, normalizedQuery))
    .sort(
      (left, right) =>
        modelPickerRank(left, selectedModelId) - modelPickerRank(right, selectedModelId),
    )
    .slice(0, 50);
}

function sortModelsForPicker(
  models: ReadonlyArray<AiModelCatalogItem>,
  selectedModelId: string,
): ReadonlyArray<AiModelCatalogItem> {
  return [...models].sort(
    (left, right) =>
      modelPickerRank(left, selectedModelId) - modelPickerRank(right, selectedModelId),
  );
}

function modelPickerRank(model: AiModelCatalogItem, selectedModelId: string): number {
  if (model.id === selectedModelId) return -3;
  if (model.default) return -2;
  if (model.lifecycle === "stable") return 0;
  if (model.lifecycle === "preview") return 1;
  return 2;
}

function modelMatchesNormalizedQuery(model: AiModelCatalogItem, normalizedQuery: string): boolean {
  const terms = normalizedQuery.split(" ").filter((term) => term.length > 0);
  if (terms.length === 0) return true;

  const haystack = normalizeSearch(modelComboboxLabel(model));
  return terms.every((term) => haystack.includes(term));
}

function providerRailClass(active: boolean): string {
  const base =
    "grid size-10 shrink-0 place-items-center rounded-2xl text-xs font-semibold transition-colors";
  return active
    ? `${base} bg-primary text-primary-foreground shadow-sm`
    : `${base} text-muted-foreground hover:bg-foreground/10 hover:text-foreground`;
}

function providerInitials(name: string): string {
  return name
    .split(/\s+/)
    .filter((part) => part.length > 0)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() ?? "")
    .join("");
}

function modelSummary(model: AiModelCatalogItem): string {
  return [
    model.contextWindow > 0 ? formatContextWindow(model.contextWindow) : undefined,
    model.capabilities.reasoning ? "Thinking" : undefined,
    model.inputModalities.includes("image") ? "Images" : undefined,
    model.capabilities.tools ? "Tools" : undefined,
  ]
    .filter((part): part is string => part !== undefined)
    .join(" · ");
}

function modelComboboxLabel(model: AiModelCatalogItem): string {
  return [
    model.displayProvider.name,
    model.displayProvider.id,
    model.name,
    model.id,
    model.family,
    model.api,
    model.lifecycle,
    routingProviderName(model),
    model.capabilities.reasoning ? "thinking reasoning" : "",
    model.capabilities.tools ? "tools" : "",
    model.inputModalities.join(" "),
  ].join(" ");
}

function routingProviderName(model: AiModelCatalogItem): string {
  const [provider] = model.id.split("/", 1);
  if (provider === "openrouter") return "via OpenRouter";
  if (provider === undefined || provider.length === 0) return "Direct";
  return `via ${provider.slice(0, 1).toUpperCase()}${provider.slice(1)}`;
}

function formatContextWindow(tokens: number): string {
  if (tokens >= 1_000_000) return `${Math.round(tokens / 1_000_000)}M context`;
  if (tokens >= 1_000) return `${Math.round(tokens / 1_000)}K context`;
  return `${tokens} context`;
}

function normalizeSearch(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
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
  if (!trimmed || trimmed.includes("@")) return undefined;
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
