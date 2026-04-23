/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: false, // Disabling Strict Mode

  eslint: {
    ignoreDuringBuilds: true, // Disable ESLint during builds on Vercel
  },
};

export default nextConfig;
