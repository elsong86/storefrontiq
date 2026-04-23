'use client';

import React, { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import Image from 'next/image';

const apiUrl = process.env.NEXT_PUBLIC_API_URL;
console.log("API URL:", apiUrl);

const SignInPage: React.FC = () => {
  const router = useRouter();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [isClient, setIsClient] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Indicate that the component has mounted on the client
    setIsClient(true);
  }, []);

  const handleSignin = async (e: React.FormEvent) => {
    e.preventDefault();
    console.log("handleSignin triggered");
  
    try {
      console.log("Preparing to send POST request to", `${apiUrl}/signin`);
      const response = await fetch(`${apiUrl}/signin`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ email, password }),
        credentials: 'include',  // This ensures cookies are sent with the request
      });
      console.log("Response received:", response);
  
      if (response.status === 429) {
        throw new Error('Too many requests. Please try again later.');
      }
  
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Sign in failed');
      }
  
      // Wait briefly to ensure the cookie is set before redirecting
      await new Promise((resolve) => setTimeout(resolve, 100));
  
      // Dispatch the authChange event
      const authChangeEvent = new Event('authChange');
      window.dispatchEvent(authChangeEvent);
  
      // Redirect to home or dashboard
      window.location.href = '/'; // Use window.location.href for full page reload
    } catch (error: any) {
      console.error('Sign in failed:', error.message);
      setError(error.message);  // Display an error message to the user
    }
  };

  if (!isClient) {
    // Prevent rendering on the server to avoid mismatches
    return null;
  }

  return (
    <section className="">
      <div className="lg:grid lg:min-h-screen lg:grid-cols-12">
        <aside className="relative block h-0 lg:order-first lg:col-span-5 lg:h-full xl:col-span-6">
          <Image
            src="/images/pexels-nubikini-1178991.jpg"
            alt="Neon Sign saying tacos"
            layout="fill"
            objectFit="cover"
            priority
            quality={100}
            className="absolute inset-0 "
          />
        </aside>

        <main className="flex items-center justify-center px-8 py-6 sm:px-12 lg:col-span-7 lg:px-16 lg:py-8 xl:col-span-6">
          <div className="max-w-xl lg:max-w-3xl">
            <Link
              href={{ pathname: '/' }}
              className="flex items-center text-lg space-x-2 group"
            >
              <Image
                src="arrow-left-svgrepo-com.svg"
                alt="Clipart Onion"
                width={20}
                height={20}
                className="relative py-2"
                priority
              />
              <span className="group-hover:text-emerald-600 transition-colors">Home</span>
            </Link>
            <Image
              src="/logo.svg"
              alt="Clipart Taco"
              width={100}
              height={100}
              className="ml-4 inline-block"
              priority
            />
            <h1 className="mt-6 text-2xl font-bold text-gray-900 sm:text-3xl md:text-4xl">
              Log In to 
              <span className="text-rose-800"> T</span>
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
              !
            </h1>
          
            <form onSubmit={handleSignin} className="mt-8 grid grid-cols-6 gap-6">
              <div className="col-span-6">
                <label className="block text-sm font-medium text-gray-700" htmlFor="email">
                  Email
                </label>
                <input
                  className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                  id="email"
                  type="email"
                  placeholder="Enter your email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                />
              </div>

              <div className="col-span-6">
                <label className="block text-sm font-medium text-gray-700" htmlFor="password">
                  Password
                </label>
                <input
                  className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline"
                  id="password"
                  type="password"
                  placeholder="Enter your password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
              </div>

              {error && (
                <p className="text-red-500 text-sm mb-4 whitespace-nowrap">{error}</p>
              )}

              <div className="col-span-6 sm:flex sm:items-center sm:gap-4">
                <button
                  className="inline-block px-4 py-2 text-slate-100 bg-black hover:bg-gradient-to-r from-rose-700 via-orange-700 to-yellow-600 transition  hover:shadow-2xl font-semibold text-md rounded shadow-xl"
                  type="submit"
                >
                  Sign In
                </button>
              </div>
            </form>
            <p className="mt-6">
              Don't have an account?{' '}
              <Link href="/signup" className="text-gray-700 underline hover:text-rose-700">
                Sign Up
              </Link>
            </p>
          </div>
        </main>
      </div>
    </section>
  );
};

export default SignInPage;
