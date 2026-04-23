// app/page.tsx
import React from 'react';
import { cookies } from 'next/headers'; // Server-only import
import HomePageClient from './components/HomePageClient';

const HomePage: React.FC = () => {
  // Server-side: Fetch the access_token from cookies
  const cookieStore = cookies();
  const token = cookieStore.get('access_token');

  // Determine if the user is authenticated (server-side)
  const initialIsAuthenticated = !!token;

  console.log('HomePage isAuthenticated:', initialIsAuthenticated);

  return (
    // Render the Client Component, passing initial authentication state
    <HomePageClient initialIsAuthenticated={initialIsAuthenticated} />
  );
};

export default HomePage;
