import * as ClientApi from "@denora/server/client-api";
import * as Linking from "expo-linking";
import * as WebBrowser from "expo-web-browser";
import * as MobileApi from "./Api.ts";
import * as MobileConfig from "./Config.ts";

WebBrowser.maybeCompleteAuthSession();

export interface AuthRedirectResult {
  readonly sessionStored: boolean;
  readonly status: string | null;
}

export class AuthRedirectError extends Error {
  override readonly name = "AuthRedirectError";

  constructor(readonly code: string) {
    super(`Authentication failed: ${code}`);
  }
}

export const makeRedirectUrl = () => Linking.createURL("auth");

export const makeAuthUrls = () => ClientApi.makeDenoraAuthUrls(MobileConfig.requireApiUrl());

const authParamsFromUrl = (url: string) => {
  const parsed = new URL(url);
  const fragment = parsed.hash.startsWith("#") ? parsed.hash.slice(1) : "";
  return new URLSearchParams(fragment.length > 0 ? fragment : parsed.search.slice(1));
};

export async function completeAuthRedirect(url: string): Promise<AuthRedirectResult> {
  const params = authParamsFromUrl(url);
  const authError = params.get("authError");
  if (authError) throw new AuthRedirectError(authError);

  const session = params.get("session");
  if (session) {
    await MobileApi.setSession(session);
  }

  return {
    sessionStored: session !== null,
    status: params.get("authStatus"),
  };
}

export async function signIn(): Promise<AuthRedirectResult | null> {
  const redirectUrl = makeRedirectUrl();
  const authUrls = makeAuthUrls();
  const result = await WebBrowser.openAuthSessionAsync(authUrls.login(redirectUrl), redirectUrl);

  if (result.type !== "success") return null;
  return completeAuthRedirect(result.url);
}

export async function signOutLocally(): Promise<void> {
  await MobileApi.clearSession();
}
