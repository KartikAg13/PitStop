type DynamicRoutes = {
	
};

type Layouts = {
	"/": undefined;
	"/qualifying": undefined
};

export type RouteId = "/" | "/qualifying";

export type RouteParams<T extends RouteId> = T extends keyof DynamicRoutes ? DynamicRoutes[T] : Record<string, never>;

export type LayoutParams<T extends RouteId> = Layouts[T] | Record<string, never>;

export type Pathname = "/" | "/qualifying";

export type ResolvedPathname = `${"" | `/${string}`}${Pathname}`;

export type Asset = "/red-bull.avif" | "/robots.txt";