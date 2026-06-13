const rawApiUrl = import.meta.env.VITE_API_URL;

export const apiUrl = rawApiUrl?.replace(/\/+$/, "") ?? "";

export const missingApiUrl = (): never => {
  throw new Error("VITE_API_URL is not configured");
};

export const requireApiUrl = () => {
  if (apiUrl.length === 0) {
    return missingApiUrl();
  }

  return apiUrl;
};

export * as WebConfig from "./WebConfig.ts";
