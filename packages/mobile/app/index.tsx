import { RegistryProvider, useAtomRefresh, useAtomSet, useAtomValue } from "@effect/atom-react";
import { StatusBar } from "expo-status-bar";
import type * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import { useEffect, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import * as MobileAtoms from "../runtime/Atoms.ts";
import * as MobileConfig from "../runtime/Config.ts";

export default function App() {
  return (
    <RegistryProvider>
      <Home />
    </RegistryProvider>
  );
}

function Home() {
  const emptyRuntime = useAtomValue(MobileAtoms.emptyRuntime);
  const runtime = useAtomValue(MobileAtoms.runtime);
  const health = useAtomValue(MobileAtoms.health);
  const refreshHealth = useAtomRefresh(MobileAtoms.health);
  const account = useAtomValue(MobileAtoms.loadAccount);
  const triggerLoadAccount = useAtomSet(MobileAtoms.loadAccount, { mode: "promise" });
  const resetAccount = useAtomSet(MobileAtoms.loadAccount);
  const signInResult = useAtomValue(MobileAtoms.signIn);
  const signInWithWorkOs = useAtomSet(MobileAtoms.signIn, { mode: "promise" });
  const signOutResult = useAtomValue(MobileAtoms.signOut);
  const signOutLocally = useAtomSet(MobileAtoms.signOut, { mode: "promise" });
  const [notice, setNotice] = useState<string | null>(null);
  const apiConfigured = MobileConfig.apiUrl.length > 0;

  useDebugAtomResult("emptyRuntime", emptyRuntime);
  useDebugAtomResult("runtime", runtime);
  useDebugAtomResult("health", health);
  useDebugAtomResult("account", account);
  useDebugAtomResult("signIn", signInResult);
  useDebugAtomResult("signOut", signOutResult);

  const user = successValue(account);
  const busy = health.waiting || account.waiting || signInResult.waiting || signOutResult.waiting;
  const message =
    notice ??
    failureMessage("runtime", runtime) ??
    failureMessage("account", account) ??
    failureMessage("sign-in", signInResult) ??
    failureMessage("sign-out", signOutResult) ??
    failureMessage("health", health) ??
    (user
      ? "Signed in."
      : successValue(health)
        ? "Backend is reachable."
        : "Configure the API URL, then sign in with WorkOS.");

  function checkHealth() {
    setNotice("Checking backend.");
    refreshHealth();
  }

  async function handleLoadAccount() {
    setNotice(null);
    try {
      await triggerLoadAccount(undefined);
      setNotice("Signed in.");
    } catch (error) {
      setNotice(errorMessage(error, "Unable to load account."));
    }
  }

  async function signIn() {
    if (!apiConfigured) {
      Alert.alert("Missing API URL", "Set EXPO_PUBLIC_API_URL to your Alchemy server URL.");
      return;
    }

    setNotice("Opening WorkOS.");
    try {
      const result = await signInWithWorkOs(undefined);
      if (!result) {
        setNotice("Sign in was cancelled.");
        return;
      }

      setNotice(result.sessionStored ? "Session stored securely." : "Returned from sign in.");
      await handleLoadAccount();
    } catch (error) {
      setNotice(errorMessage(error, "Sign in failed."));
    }
  }

  async function signOut() {
    try {
      await signOutLocally(undefined);
      resetAccount(MobileAtoms.Reset);
      setNotice("Local session cleared.");
    } catch (error) {
      setNotice(errorMessage(error, "Unable to clear local session."));
    }
  }

  return (
    <SafeAreaView style={styles.shell}>
      <StatusBar style="light" />
      <ScrollView contentContainerStyle={styles.content}>
        <View style={styles.hero}>
          <Text style={styles.eyebrow}>Denora</Text>
          <Text style={styles.title}>Create and talk to your agent.</Text>
          <Text style={styles.subtitle}>
            This is the iOS-first shell: WorkOS auth, typed Effect API calls, and native redirect
            handling.
          </Text>
        </View>

        <View style={styles.card}>
          <Text style={styles.label}>API</Text>
          <Text style={styles.value}>
            {MobileConfig.apiUrl || "EXPO_PUBLIC_API_URL is not set"}
          </Text>
          <Text style={styles.label}>Health</Text>
          <Text style={styles.value}>{successValue(health)?.status ?? "unknown"}</Text>
          <Text style={styles.label}>Status</Text>
          <Text style={styles.value}>{message}</Text>
        </View>

        {user ? (
          <View style={styles.card}>
            <Text style={styles.label}>Signed In As</Text>
            <Text style={styles.value}>{user.name ?? user.email}</Text>
            <Text style={styles.secondary}>{user.email}</Text>
          </View>
        ) : null}

        <View style={styles.actions}>
          <Button disabled={busy || !apiConfigured} label="Sign in with WorkOS" onPress={signIn} />
          <Button
            disabled={busy || !apiConfigured}
            label="Load account"
            onPress={handleLoadAccount}
            secondary
          />
          <Button disabled={busy} label="Clear local session" onPress={signOut} secondary />
          <Button
            disabled={busy || !apiConfigured}
            label="Check backend"
            onPress={checkHealth}
            secondary
          />
        </View>

        {busy ? <ActivityIndicator color="#f3ede2" /> : null}
      </ScrollView>
    </SafeAreaView>
  );
}

function successValue<A, E>(result: AsyncResult.AsyncResult<A, E>): A | null {
  return result._tag === "Success" ? result.value : null;
}

function failureMessage(
  name: string,
  result: AsyncResult.AsyncResult<unknown, unknown>,
): string | null {
  if (result._tag !== "Failure") return null;
  return `${name}: ${causeMessage(result.cause) ?? safeJson(result)}`;
}

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  return fallback;
}

function causeMessage(cause: unknown): string | null {
  if (!isRecord(cause)) return errorMessage(cause, "Request failed.");

  const reasons = cause.reasons;
  if (!Array.isArray(reasons)) return safeJson(cause);

  for (const reason of reasons) {
    if (!isRecord(reason)) continue;
    const value = reason.error ?? reason.defect;
    const message = errorMessage(value, "");
    if (message.length > 0) return message;
    const tag = typeof reason._tag === "string" ? reason._tag : "failure";
    return `${tag}: ${safeJson(reason)}`;
  }

  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function useDebugAtomResult(name: string, result: AsyncResult.AsyncResult<unknown, unknown>) {
  useEffect(() => {
    if (result._tag !== "Failure") return;

    console.log(`[denora:atom:${name}] failure ${safeJson(inspectFailure(result))}`);
  }, [name, result]);
}

function inspectFailure(result: AsyncResult.AsyncResult<unknown, unknown>) {
  if (result._tag !== "Failure") return result;

  return {
    cause: inspectUnknown(result.cause),
    result: inspectUnknown(result),
    resultKeys: Object.keys(result),
    resultOwnProperties: Object.getOwnPropertyNames(result),
    resultPrototype: Object.getPrototypeOf(result)?.constructor?.name,
    resultTag: result._tag,
    resultString: String(result),
    waiting: result.waiting,
  };
}

function inspectUnknown(value: unknown): unknown {
  if (value instanceof Error) {
    return {
      message: value.message,
      name: value.name,
      stack: value.stack,
    };
  }

  if (!isRecord(value)) return value;

  return {
    keys: Object.keys(value),
    prototype: Object.getPrototypeOf(value)?.constructor?.name,
    string: String(value),
    value,
  };
}

function Button(props: {
  readonly disabled?: boolean;
  readonly label: string;
  readonly onPress: () => void;
  readonly secondary?: boolean;
}) {
  return (
    <Pressable
      disabled={props.disabled}
      onPress={props.onPress}
      style={({ pressed }) => [
        styles.button,
        props.secondary ? styles.secondaryButton : null,
        props.disabled ? styles.disabledButton : null,
        pressed ? styles.pressedButton : null,
      ]}
    >
      <Text style={[styles.buttonText, props.secondary ? styles.secondaryButtonText : null]}>
        {props.label}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  actions: {
    gap: 12,
  },
  button: {
    alignItems: "center",
    backgroundColor: "#f3ede2",
    borderRadius: 18,
    paddingHorizontal: 18,
    paddingVertical: 15,
  },
  buttonText: {
    color: "#161411",
    fontSize: 16,
    fontWeight: "700",
  },
  card: {
    backgroundColor: "rgba(255, 255, 255, 0.08)",
    borderColor: "rgba(255, 255, 255, 0.14)",
    borderRadius: 26,
    borderWidth: 1,
    gap: 7,
    padding: 20,
  },
  content: {
    gap: 22,
    padding: 22,
    paddingBottom: 40,
  },
  disabledButton: {
    opacity: 0.45,
  },
  eyebrow: {
    color: "#b9a98f",
    fontSize: 14,
    fontWeight: "800",
    letterSpacing: 2,
    textTransform: "uppercase",
  },
  hero: {
    gap: 12,
    paddingTop: 32,
  },
  label: {
    color: "#b9a98f",
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 1,
    textTransform: "uppercase",
  },
  pressedButton: {
    transform: [{ scale: 0.99 }],
  },
  secondary: {
    color: "#b9a98f",
    fontSize: 15,
  },
  secondaryButton: {
    backgroundColor: "transparent",
    borderColor: "rgba(243, 237, 226, 0.3)",
    borderWidth: 1,
  },
  secondaryButtonText: {
    color: "#f3ede2",
  },
  shell: {
    backgroundColor: "#161411",
    flex: 1,
  },
  subtitle: {
    color: "#d2c7b6",
    fontSize: 18,
    lineHeight: 26,
  },
  title: {
    color: "#fff9ef",
    fontSize: 43,
    fontWeight: "800",
    letterSpacing: -1.4,
    lineHeight: 47,
  },
  value: {
    color: "#fff9ef",
    fontSize: 17,
    lineHeight: 24,
  },
});
