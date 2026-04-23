// app/components/HomePageClient.tsx
"use client"; // Mark this as a Client Component

import React, { useState, useEffect } from 'react';
import Footer from './Footer';
import Header from './NewHeader'
import SearchContainerNew from './SearchContainerNew'
import HowitWorks from './HowItWorks'

interface HomePageClientProps {
  initialIsAuthenticated: boolean;
}

const HomePageClient: React.FC<HomePageClientProps> = ({ initialIsAuthenticated }) => {
  // Client-side state to manage authentication after page load
  const [isAuthenticated, setIsAuthenticated] = useState(initialIsAuthenticated);

  // Client-side check to handle authentication state updates (like logging in/out)
  useEffect(() => {
    const checkAuthStatus = () => {
      // Since we can't access HTTP-only cookies, we'll rely on events or API calls
      // For simplicity, we'll assume an event is dispatched on auth change
      setIsAuthenticated(document.cookie.includes('access_token'));
    };

    // Listen for login/logout events and check authentication status
    window.addEventListener('authChange', checkAuthStatus);

    return () => {
      window.removeEventListener('authChange', checkAuthStatus);
    };
  }, []);

  console.log('HomePageClient isAuthenticated:', isAuthenticated);

  return (
    <div>
      {/* Pass the updated authentication state to Header */}
      <Header initialIsAuthenticated={isAuthenticated}/>
      {/* Rest of your page content */}
      
      <div>
        <SearchContainerNew />
        <HowitWorks />
      </div>
      <Footer />
    </div>
  );
};

export default HomePageClient;
