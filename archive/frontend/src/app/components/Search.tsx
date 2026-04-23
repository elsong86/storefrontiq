'use client';

import React from 'react';
import { Location } from '../types';

interface HeaderProps {
  onLocationShare: (location: Location) => void;
  onAddressSubmit: (address: string) => void;
}

const Search: React.FC<HeaderProps> = ({
  onLocationShare,
  onAddressSubmit,
}) => {
  const handleLocationShare = () => {
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          const { latitude, longitude } = position.coords;
          onLocationShare({ latitude, longitude });
        },
        (error) => {
          console.error('Error getting location:', error);
        }
      );
    } else {
      console.error('Geolocation is not supported by this browser.');
    }
  };

  const handleAddressSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const form = e.target as HTMLFormElement;
    const formData = new FormData(form);
    const address = formData.get('address') as string;
    onAddressSubmit(address);
  };

  return (
    <header className="flex flex-col items-center w-full max-w-md mx-auto space-y-4">
    {/* Share Location Button */}
    <button
      onClick={handleLocationShare}
      aria-label="Share your current location"
      className="w-full py-2 rounded bg-black text-white font-semibold hover:bg-gradient-to-r from-rose-700 via-orange-700 to-yellow-600 transition shadow-2xl"
    >
      Share Location
    </button>

    {/* Divider */}
    <span className="text-black ">OR</span>

    {/* Search Form */}
    <form onSubmit={handleAddressSubmit} className="flex items-center w-full space-x-2">
      <input
        name="address"
        type="text"
        placeholder="Enter address"
        aria-label="Address"
        required
        className="flex-grow bg-gray-100/60 rounded border-2 border-gray-300 p-2 focus:outline-none focus:ring-2 focus:ring-lime-500 shadow-2xl"
      />
      <button
        type="submit"
        className="py-2 px-4 rounded bg-black text-white font-semibold hover:bg-gradient-to-r from-green-600 to-lime-600 transition shadow-2xl"
      >
        Search
      </button>
    </form>
  </header>
  );
};

export default Search;
