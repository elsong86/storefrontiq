import React from "react";
import Link from "next/link";
import Image from "next/image";




const Footer: React.FC = () => {
  return (
    <section>
      {/* Container */}
      <div className="mx-auto w-full max-w-7xl px-5 py-16 md:px-10 md:py-8">
      
        {/* Border line */}
      <div className="mb-14 w-full border-b border-grey-600 mt-16"></div>
        {/* Title */}
        <h2 className="text-center text-3xl font-bold md:text-5xl">
          <p className="block"> 
            <span className="text-rose-800">H</span>
            <span className="text-emerald-800">O</span>
            <span className="text-yellow-600">W </span>
            
            <span className="text-orange-700"> I</span>
            <span className="text-rose-800">T </span>
            
            <span className="text-emerald-800">W</span>
            <span className="text-yellow-600">O</span>
            <span className="text-rose-800">R</span>
            <span className="text-orange-700">K</span>
            <span className="text-emerald-800">S</span>
         </p>
        </h2>
        <p className="mx-auto mb-8 mt-4 max-w-lg text-center text-xl text-gray-500  md:mb-12 lg:mb-16 ">
          Using Taco About it is really easy! 
        </p>
        {/* Content */}
        <div className="grid gap-6 sm:grid-cols-2 md:grid-cols-3 lg:gap-7">

          {/* Item shadow-red-300 */}

          <div className="relative grid gap-4 rounded-md border border-solid border-gray-300 shadow-[0px_1px_41px_-5px_#fca5a5] p-8 md:p-10">
          <Image
              src="/chili-pepper-svgrepo-com.svg"
              alt="Clipart chili"
              width={100}
              height={100}
              className="absolute top-0 right-0 h-20 w-20 m-4 "
              priority
            />
            <div className="flex h-12 w-12 items-center justify-center rounded-full bg-red-100">
              <p className="text-md font-bold sm:text-xl">1</p>
            </div> 
            <p className="text-xl font-semibold">Share Your Location</p>
            <p className="text-sm text-gray-500">
              Click the button to share, or manually enter an address.
            </p>
          </div>


          {/* Item  shadow-lime-300*/}
          <div className="relative grid gap-4 rounded-md border border-solid border-gray-300 shadow-[0px_1px_41px_-5px_#bef264] p-8 md:p-10">
          <Image
              src="avocado-svgrepo-com.svg"
              alt="Clipart Avocado"
              width={100}
              height={100}
             className="absolute top-0 right-0 h-20 w-20 m-4 "
              priority
            />
            <div className="flex h-12 w-12 items-center justify-center rounded-full bg-lime-100">
              <p className="text-sm font-bold sm:text-xl">2</p>
              
            </div>
            
            
            <p className="text-xl font-semibold">Read Through The Reviews</p>
            <p className="text-md text-gray-500">
              Wait for all of the results to pop up on the next page, and click on the ones that interest you. 
            </p>
          </div>
          {/* Item  shadow-yellow-300 */}
          <div className="relative grid gap-4 rounded-md border border-solid border-gray-300 shadow-[0px_1px_41px_-5px_#fde047] p-8 md:p-10">
          <Image
              src="corn-svgrepo-com.svg"
              alt="Clipart Onion"
              width={50}
              height={50}
              className="absolute top-0 right-0 h-20 w-20 m-4 "
              priority
            />
            <div className="flex h-12 w-12 items-center justify-center rounded-full bg-yellow-100">
              <p className="text-sm font-bold sm:text-xl">3</p>
            </div>
            <p className="text-xl font-semibold">Sign Up For More</p>
            <p className="text-md text-gray-500">
              Go and get your delicious tacos. But don't forget to sign up for premium features!
            </p>
          </div>
        </div>
         {/* Border line */}
      {/* <div className="mb-2 w-full border-b border-grey-600 mt-20"></div> */}
      </div>
    </section>
  )
};

export default Footer;