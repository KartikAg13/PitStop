<script lang="ts">
  import { Home, User } from 'lucide-svelte';
  
  let activeItem = $state('home');
  
  function setActive(item: string) {
    activeItem = item;
  }
</script>

<!-- Fullscreen blurred background -->
<div class="absolute inset-0 z-[-1]">
  <img 
    src="/red-bull.avif" 
    alt="Background"
    class="w-full h-full object-cover blur-xs brightness-75" 
  />
</div>

<div class="h-20 flex items-center justify-center">
  <nav 
    class="
      relative 
      bg-white/10           
      bg-clip-padding        
      backdrop-blur-lg 
      border                 
      border-white/15   
      shadow-lg              
      rounded-full 
    "
  >
    <!-- Animated background indicator -->
    <div 
      class="
        absolute inset-y-1 
        bg-white/15
        rounded-full 
        transition-all 
        duration-300 
        ease-out 
        shadow-md
      "
      style="width: calc(50% - 4px); left: {activeItem === 'home' ? '4px' : 'calc(50% + 0px)'}"
    ></div>
    
    <ul class="flex items-center relative z-10">
      <li class="flex-1">
        <button
          onclick={() => setActive('home')}
          class="
            flex items-center justify-center space-x-2 
            px-5 py-2 rounded-full 
            text-white/80 hover:text-white 
            transition-all duration-300 transform hover:scale-105 
            {activeItem === 'home' ? 'text-white' : ''}
          "
          type="button"
        >
          <Home 
            size={18} 
            class="transition-transform duration-300 {activeItem === 'home' ? 'scale-110' : ''}" 
          />
          <span class="font-medium whitespace-nowrap">Home</span>
        </button>
      </li>
      <li class="flex-1">
        <button
          onclick={() => setActive('about')}
          class="
            flex items-center justify-center content-evenly space-x-2 
            px-5 py-2 rounded-full 
            text-white/80 hover:text-white 
            transition-all duration-300 transform hover:scale-105 
            {activeItem === 'about' ? 'text-white' : ''}
          "
          type="button"
        >
          <User 
            size={18} 
            class="transition-transform duration-300 {activeItem === 'about' ? 'scale-110' : ''}" 
          />
          <span class="font-medium whitespace-nowrap">About Me</span>
        </button>
      </li>
    </ul>
    
    <!-- Glass shine overlay -->
    <div class="absolute inset-0 rounded-full bg-gradient-to-r from-white/5 via-white/10 to-transparent pointer-events-none"></div>
  </nav>
</div>
