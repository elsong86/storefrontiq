// app/components/Header.tsx

"use client"; // Mark this as a Client Component

import React from 'react';
import { useState } from 'react';
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

  // mobile screens
  const [isOpen, setIsOpen] = useState(false);

  return (
    <section>
      <nav className="font-inter mx-auto h-auto w-full max-w-screen-4xl lg:relative lg:top-0 shadow-xl bg-stone-100">
        <div className="flex flex-col px-6 py-6 lg:flex-row lg:items-center lg:justify-between lg:px-10 lg:py-4 xl:px-20">
          <a className="text-3xl sm:text-3xl md:text-3xl lg:text-6xl font-hustlers">
            <Image
              src="/logo.svg"
              alt="Clipart Taco"
              width={50}
              height={50}
              className="mr-4 inline-block"
              priority
            />
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
          </a>

          <div
            className={`flex flex-col space-y-4 lg:flex lg:flex-row lg:space-x-3 lg:space-y-0 text-center sm:text-left py-1 bg-inherit  ${isOpen ? "" : "hidden"}`}
          >
            {initialIsAuthenticated ? (
              <>
                <Link
                  href="/profile"
                  className="inline-block px-4 py-2 text-slate-100 bg-black hover:bg-gradient-to-r from-yellow-600 via-green-700 to-teal-600 transition  hover:shadow-2xl font-semibold text-md rounded shadow-xl text-center"
                >
                  Profile
                </Link>
                <button
                  onClick={handleLogout}
                  className="inline-block px-4 py-2 text-black hover:text-rose-600 font-semibold border rounded text-md text-center"
                >
                  Logout
                </button>
              </>
            ) : (
              <>
              <Link
                  href="/signup"
                  className="inline-block px-4 py-2 text-slate-100 bg-black hover:bg-gradient-to-r from-rose-700 via-orange-700 to-yellow-600 transition  hover:shadow-2xl font-semibold text-md rounded shadow-xl text-center"
                >
                  Sign up
                </Link>
                <Link
                  href="/signin"
                  className="inline-block px-4 py-2 text-black hover:text-emerald-600 font-semibold border rounded text-md text-center"
                >
                  Log In
                </Link>
                
              </>
            )}
          </div>

          {/* For mobile screens */}
          <button
            className="absolute right-5 lg:hidden"
            onClick={() => {
              setIsOpen(!isOpen);
            }}
          >
            <svg
              width="35"
              height="35"
              viewBox="0 0 24 24"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
            >
              <path
                d="M3.75 12H20.25"
                stroke="#9f1239"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              ></path>
              <path
                d="M3.75 6H20.25"
                stroke="#065f46"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              ></path>
              <path
                d="M3.75 18H20.25"
                stroke="#ca8a04"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              ></path>
            </svg>
          </button>
        </div>
      </nav>
    </section>
  );

};

export default Header;
