'use client';

import React, { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Search from './Search';
import { handleLocationShare } from '../services/locationService';
import { handleAddressSubmit } from '../services/geocodeService';
import { trackVisit } from '../services/analytics';
import { Location } from '../types';

const SearchContainer: React.FC = () => {
  const router = useRouter();

  useEffect(() => {
    trackVisit();
  }, []);

  return (
    <div className="relative flex flex-col items-center rounded-lg border border-gray-300 border-opacity-50 text-center shadow-2xl bg-gray-200 bg-opacity-50 my-10 pt-20 pb-10 px-10">
      <p className=" font-bold text-3xl">
        ~ Anywhere, Anytime ~
      </p>

      <h1 className="text-6xl  py-4 text-neutral-950">
        FIND YOUR NEW FAVORITE <br />TACO SPOT!
      </h1>

      <p className="font-bold text-xl pb-5">
        Share or enter your location to get started.
      </p>

      <Search
        onLocationShare={(loc: Location) => handleLocationShare(loc, router)}
        onAddressSubmit={(address: string) =>
          handleAddressSubmit(address, router)
        }
      />
    </div>
  );
};

export default SearchContainer;
