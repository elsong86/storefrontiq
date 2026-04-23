'use client';
import { useEffect, useState } from 'react';

interface User {
  user_id: string;
  email: string;
}

const apiUrl = process.env.NEXT_PUBLIC_API_URL;

const Profile: React.FC = () => {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchUserProfile = async () => {
      try {
        const response = await fetch(`${apiUrl}/profile`, {
          credentials: 'include',  // Make sure cookies are sent with the request
        });
        
        if (!response.ok) {
          throw new Error('Failed to fetch user profile');
        }
  
        const data = await response.json();
        setUser(data);
      } catch (err: any) {
        setError(err.message || 'Unexpected error occurred');
      } finally {
        setLoading(false);
      }
    };
  
    fetchUserProfile();
  }, []);

  if (loading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <div>Error: {error}</div>;
  }

  return (
    <div>
      <h1>User Profile</h1>
      {user ? (
        <div>
          <p><strong>Email:</strong> {user.email}</p>
        </div>
      ) : (
        <div>No user data available</div>
      )}
    </div>
  );
};

export default Profile;
