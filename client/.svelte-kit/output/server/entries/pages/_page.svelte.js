import "clsx";
function _page($$payload) {
  $$payload.out.push(`<div class="absolute inset-0 z-[-1]"><img src="/red-bull.avif" alt="Background" class="w-full h-full object-cover blur-xs brightness-75"/></div> <div class="flex flex-col items-center justify-center h-[calc(100vh-5rem)] text-white gap-8"><h1 class="text-4xl font-bold drop-shadow-lg text-center">What do you want to do?</h1> <div class="flex flex-row gap-6"><a href="/qualifying"><button class="px-6 py-3 bg-white/10 text-white border border-white/20 rounded-xl shadow-md backdrop-blur-lg hover:bg-white/20 transition-all">Predict Grid</button></a> <button class="px-6 py-3 bg-white/10 text-white border border-white/20 rounded-xl shadow-md backdrop-blur-lg hover:bg-white/20 transition-all">Race Simulation</button></div></div>`);
}
export {
  _page as default
};
