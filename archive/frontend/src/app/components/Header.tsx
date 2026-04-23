"use client"; // Mark this as a Client Component

import React from 'react';
import Link from 'next/link'; // Import Next.js Link component
import Image from 'next/image'; // Import the Image component

interface HeaderProps {
  initialIsAuthenticated: boolean; // Define the prop type for initialIsAuthenticated
}

const apiUrl = process.env.NEXT_PUBLIC_API_URL;

const Header: React.FC<HeaderProps> = ({ initialIsAuthenticated }) => {
  const handleLogout = async () => {
    try {
      // Use the fetch API to send a POST request to the logout route
      const response = await fetch(`${apiUrl}/logout`, {
        method: 'POST',
        credentials: 'include', // Include cookies in the request
      });

      if (response.ok) {
        // Dispatch a custom event to notify the app of the authentication change
        const authChangeEvent = new Event('authChange');
        window.dispatchEvent(authChangeEvent);

        // Redirect the user to the home page after successful logout
        window.location.href = '/';
      } else {
        console.error('Logout failed:', response.statusText);
      }
    } catch (error) {
      console.error('Error logging out:', error);
    }
  };

  return (
    <header
      className="sticky top-0 left-0 right-0 shadow-md h-36 z-10 py-4 flex items-center text-center"
    >
      {/* Title */}
      <div
        className="absolute left-1/2 transform -translate-x-1/2 text-center text-4xl"
      >
        {/* Taco About It */}
        <span className="text-rose-800">T</span>
        <span className="text-emerald-800">A</span>
        <span className="text-yellow-600">C</span>
        <span className="text-orange-700">O </span>
        <span> </span>
        <span className="text-rose-800">A</span>
        <span className="text-emerald-800">B</span>
        <span className="text-yellow-600">O</span>
        <span className="text-orange-700">U</span>
        <span className="text-rose-800">T</span>
        <span> </span>
        <span className="text-emerald-800">I</span>
        <span className="text-yellow-600">T</span>
      </div>

      {/* Buttons Section */}
      <div className="flex space-x-4 ml-auto p-4">
        {initialIsAuthenticated ? (
          <>
            {/* Profile Link using Next.js Link */}
            <Link
              href="/profile"
              className="inline-block px-4 py-2 text-amber-50 bg-gray-700 hover:bg-gray-500 hover:text-gray-100 hover:shadow-lg font-bold text-md rounded shadow"
            >
              Profile
            </Link>

            {/* Logout Button */}
            <button
              onClick={handleLogout}
              className="inline-block px-4 py-2 text-amber-50 bg-red-600 hover:bg-white hover:text-red-600 hover:shadow-lg font-bold text-md rounded shadow"
            >
              Logout
            </button>
          </>
        ) : (
          <>
            <Link
              href="/signin"
              className="inline-block px-4 py-2 text-amber-50 bg-yellow-600 hover:bg-white hover:text-yellow-600 hover:shadow-lg font-bold text-md rounded shadow"
            >
              Sign In
            </Link>
            <Link
              href="/signup"
              className="inline-block px-4 py-2 text-amber-50 bg-sky-700 hover:bg-white hover:text-sky-700 hover:shadow-lg font-bold text-md rounded shadow"
            >
              Sign Up
            </Link>
          </>
        )}
      </div>
    </header>
  );
};

export default Header;
