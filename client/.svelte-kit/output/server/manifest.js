export const manifest = (() => {
function __memo(fn) {
	let value;
	return () => value ??= (value = fn());
}

return {
	appDir: "_app",
	appPath: "_app",
	assets: new Set(["red-bull.avif","robots.txt"]),
	mimeTypes: {".avif":"image/avif",".txt":"text/plain"},
	_: {
		client: {start:"_app/immutable/entry/start.CmQYk1GT.js",app:"_app/immutable/entry/app.BUOm6ewh.js",imports:["_app/immutable/entry/start.CmQYk1GT.js","_app/immutable/chunks/C9l1ByPL.js","_app/immutable/chunks/CZJB1oOI.js","_app/immutable/chunks/RBBNZMpk.js","_app/immutable/chunks/DIeogL5L.js","_app/immutable/chunks/D94H7nYV.js","_app/immutable/entry/app.BUOm6ewh.js","_app/immutable/chunks/RBBNZMpk.js","_app/immutable/chunks/DIeogL5L.js","_app/immutable/chunks/CZJB1oOI.js","_app/immutable/chunks/D94H7nYV.js","_app/immutable/chunks/DsnmJJEf.js","_app/immutable/chunks/BrT96wiW.js"],stylesheets:[],fonts:[],uses_env_dynamic_public:false},
		nodes: [
			__memo(() => import('./nodes/0.js')),
			__memo(() => import('./nodes/1.js')),
			__memo(() => import('./nodes/2.js')),
			__memo(() => import('./nodes/3.js')),
			__memo(() => import('./nodes/4.js'))
		],
		remotes: {
			
		},
		routes: [
			{
				id: "/",
				pattern: /^\/$/,
				params: [],
				page: { layouts: [0,], errors: [1,], leaf: 2 },
				endpoint: null
			},
			{
				id: "/qualifying",
				pattern: /^\/qualifying\/?$/,
				params: [],
				page: { layouts: [0,], errors: [1,], leaf: 3 },
				endpoint: null
			},
			{
				id: "/race",
				pattern: /^\/race\/?$/,
				params: [],
				page: { layouts: [0,], errors: [1,], leaf: 4 },
				endpoint: null
			}
		],
		prerendered_routes: new Set([]),
		matchers: async () => {
			
			return {  };
		},
		server_assets: {}
	}
}
})();
