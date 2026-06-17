import { Alert, AlertDescription } from "@denora/ui/components/alert";
import { Avatar, AvatarFallback } from "@denora/ui/components/avatar";
import { Badge } from "@denora/ui/components/badge";
import { Button } from "@denora/ui/components/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@denora/ui/components/card";
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@denora/ui/components/item";
import { Separator } from "@denora/ui/components/separator";
import { createFileRoute } from "@tanstack/react-router";
import { useMutation } from "@tanstack/react-query";

import { getAuthClient } from "../../auth-client.ts";

export const Route = createFileRoute("/_authenticated/app")({
  component: AppHome,
});

function AppHome() {
  const { auth } = Route.useRouteContext();
  const user = auth.user;
  const userInitial = (user.name || user.email || "D").slice(0, 1).toUpperCase();

  const signOutMutation = useMutation({
    mutationFn: async () => {
      getAuthClient().signOut();
    },
  });

  return (
    <main className="grid min-h-svh place-items-center p-4">
      <Card className="w-full max-w-md" size="sm">
        <CardHeader>
          <Badge variant="secondary">Denora</Badge>
          <CardTitle>Your agent is ready.</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-5">
          <Item variant="muted">
            <ItemMedia>
              <Avatar size="lg">
                <AvatarFallback>{userInitial}</AvatarFallback>
              </Avatar>
            </ItemMedia>
            <ItemContent>
              <ItemTitle>{user.email}</ItemTitle>
              <ItemDescription>Signed in with your WorkOS AuthKit session.</ItemDescription>
            </ItemContent>
          </Item>
          <Separator />
          <CardDescription>
            This protected route is loaded through the same session cookie the API client forwards
            to the server.
          </CardDescription>
        </CardContent>
        <CardFooter>
          <Button
            disabled={signOutMutation.isPending}
            size="lg"
            type="button"
            onClick={() => signOutMutation.mutate()}
          >
            {signOutMutation.isPending ? "Signing out..." : "Sign out"}
          </Button>
        </CardFooter>
        {signOutMutation.error instanceof Error ? (
          <CardContent>
            <Alert variant="destructive">
              <AlertDescription>{signOutMutation.error.message}</AlertDescription>
            </Alert>
          </CardContent>
        ) : null}
      </Card>
    </main>
  );
}
