import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export async function middleware(request: NextRequest) {
  // Extract the access token from the cookies
  const accessToken = request.cookies.get('access_token')?.value;

  // Log the access token for debugging
  console.log('Access Token:', accessToken);

  // Check if the user is trying to access /signin or /signup
  if (request.nextUrl.pathname === '/signin' || request.nextUrl.pathname === '/signup') {
    // If the user is signed in (has an access token), redirect them to the root ('/')
    if (accessToken) {
      console.log('User is signed in, redirecting to /...');
      return NextResponse.redirect(new URL('/', request.url));
    }
  }

  // If no access token and accessing a protected route (e.g., /profile), redirect to /signin
  if (!accessToken && request.nextUrl.pathname === '/profile') {
    console.log('No access token, redirecting to /signin...');
    return NextResponse.redirect(new URL('/signin', request.url));
  }

  // Proceed with the request if the user is authenticated or not accessing protected routes
  return NextResponse.next();
}

export const config = {
  matcher: ['/profile', '/signin', '/signup'],  // Protect the /profile, /signin, and /signup routes
};
