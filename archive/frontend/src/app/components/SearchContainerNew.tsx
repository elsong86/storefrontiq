'use client';

import React, { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Search from './Search';
import { handleLocationShare } from '../services/locationService';
import { handleAddressSubmit } from '../services/geocodeService';
import { trackVisit } from '../services/analytics';
import { Location } from '../types';
import Image from 'next/image';

const SearchContainer: React.FC = () => {
  const router = useRouter();

  useEffect(() => {
    trackVisit();
  }, []);

  return (
    
    <section
    // Since this is a background CSS - in - JS was a better option than Next.js IMG
      className="relative bg-[url(https://images.pexels.com/photos/5454020/pexels-photo-5454020.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=2)] bg-cover bg-center bg-no-repeat shadow-2xl max-h-400 mx-auto max-w-7xl px-5 py-16 md:px-10 md:py-20"
      >
      <><div
        className="absolute inset-0 md:bg-gradient-to-r md:from-white/75 md:to-transparent "
      ></div>
      {/* for small screens */}
      <div
      className="absolute inset-0 bg-white/65 sm:bg-transparent sm:from-white/65 sm:to-white/10 sm:bg-gradient-to-r"
      ></div>

      <div
          className="relative mx-auto max-w-screen-6xl px-4 py-32 sm:px-6 lg:flex lg:h-screen lg:items-center lg:px-8"
        >
            <div className="max-w-xl text-center sm:text-left ">
              
            <p className=" font-semibold text-2xl sm:text-2xl md:text-3xl lg:text-4xl font-thirsty text-center pb-4">
            ~ Anywhere, Anytime ~
            </p>

            <div>
            <h1 className=" text-4xl sm:text-4xl md:text-4xl lg:text-5xl font-brothers whitespace:wrap lg:whitespace-nowrap">
            FIND YOUR NEW FAVORITE
            </h1>

        <p className="block font-bold md:font-normal text-5xl sm:text-4xl md:text-5xl lg:text-8xl text-center"
        > 
            <span className="text-rose-800 font-brothers sm:font-hustlers">T</span>
            <span className="text-emerald-800 font-brothers sm:font-hustlers ">A</span>
            <span className="text-yellow-600 font-brothers sm:font-hustlers ">C</span>
            <span className="text-orange-700 font-brothers sm:font-hustlers ">O </span>
            
            <span className="text-rose-800 font-brothers sm:font-hustlers  ">S</span>
            <span className="text-emerald-800 font-brothers sm:font-hustlers ">P</span>
            <span className="text-yellow-600 font-brothers sm:font-hustlers ">O</span>
            <span className="text-rose-800 font-brothers sm:font-hustlers ">T</span>
            <span></span>
            {/* <Image
              src="/logo.svg"
              alt="Clipart Taco"
              width={60}
              height={60}
              className="ml-2 inline-block"
              priority
            /> */}
         </p>
         </div>
        
              <p className=" ml-0 sm:ml-4 md:ml-4 max-w-lg sm:text-xl/relaxed text-center pb-4">
                Share or enter your location to get started
              </p>

              <span className="relative flex justify-center">
                  <div
                    className="absolute inset-x-0 top-1/2 h-px -translate-y-1/2 bg-gray-500"
                  ></div>
                </span>
                              

              {/* Come back and edit this section to fit better both large and small screens- make buttons responsive  */}
              <div className="pt-6">
              <Search
              onLocationShare={(loc: Location) => handleLocationShare(loc, router)}
              onAddressSubmit={(address: string) =>
                handleAddressSubmit(address, router)
              }
            />
              </div>

            </div>
          </div></>
    </section>

  );
};

export default SearchContainer;
