// @ts-check
import { defineConfig } from "astro/config";

import tailwindcss from "@tailwindcss/vite";

import sitemap from "@astrojs/sitemap";

const site = process.env.PUBLIC_SITE_URL?.replace(/\/$/, "");

// https://astro.build/config
export default defineConfig({
  ...(site ? { site } : {}),

  vite: {
    plugins: [tailwindcss()],
  },

  integrations: [sitemap()],
});
