import React, { useState } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import Modal from './Feedback'; // Import the Modal component

const Footer: React.FC = () => {
  const [isModalOpen, setIsModalOpen] = useState(false);

  const handleOpenModal = () => {
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
  };

  return (
    <footer className="block sm:flex sm:justify-center">
      {/* Container */}
      <div className="py-16 md:py-20 mx-auto w-full max-w-7xl px-5 md:px-10">
        {/* Component */}
        
        <div className="mb-10 w-full border-b border-black mt-16"></div>
        
        <div className="text-center sm:text-left sm:flex-row flex justify-between flex-col sm:pb-2">
          <a className="text-3xl sm:text-3xl md:text-4xl lg:text-4xl font-hustlers">
            <Image
              src="/logo.svg"
              alt="Clipart Taco"
              width={50}
              height={50}
              className="mr-4 inline-block"
              priority
            />
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
          </a>
        </div>
        <div className="md:flex-row flex justify-between sm:items-center sm:flex-col items-center flex-col-reverse">
          <div className="font-semibold mb-4 sm:mb-0 py-1 text-center sm:text-center">
            {/* Goes to github */}
            <Link
              href="https://github.com/elsong86/taco-about-it"
              className="inline-block font-normal text-gray-500 transition hover:text-yellow-600 sm:pr-6 lg:pr-12 py-1.5 sm:py-2 pr-6"
            >
              About
            </Link>

            {/* Opens Modal that will send us feedback */}
            <button
              className="inline-block font-normal text-gray-500 transition hover:text-rose-600 sm:pr-6 lg:pr-12 py-1.5 sm:py-2 pr-6"
              onClick={handleOpenModal}
            >
              Feedback
            </button>

            <Link
              href={{
                pathname: '/'
              }}
              className="inline-block font-normal text-gray-500 transition hover:text-emerald-600 sm:pr-6 lg:pr-12 py-1.5 sm:py-2 pr-6"
            >
              Home
            </Link>

            {/* Links to medium article */}
            <Link
              href="#"
              className="inline-block font-normal text-gray-500 transition hover:text-orange-600 sm:pr-6 lg:pr-12 py-1.5 sm:py-2 pr-6"
            >
              Blog
            </Link>
          </div>

          {/* Right hand section */}
          <div>
            <ul className="col-span-2 flex justify-center gap-6 lg:col-span-5 lg:justify-end pt-2">
              <li>
                <Link
                  href="https://github.com/elsong86"
                  className="text-green-500"
                >
                  <Image
                    src="/github.svg"
                    alt="Github logo"
                    width={20}
                    height={20}
                    className="mr-4 inline-block"
                    priority
                  />
                </Link>
              </li>

              <li>
                <Link
                  href="https://www.linkedin.com/in/ellissong/"
                  className=""
                >
                  <Image
                    src="/linkedin.svg"
                    alt="linkedin Logo"
                    width={20}
                    height={20}
                    className="mr-4 inline-block"
                    priority
                  />
                </Link>
              </li>

              <li>
                <Link
                  href="https://github.com/sarhiri"
                  className=""
                >
                  <Image
                    src="/github.svg"
                    alt="GitHub Logo"
                    width={20}
                    height={20}
                    className="mr-4 inline-block"
                    priority
                  />
                </Link>
              </li>

              <li>
                <Link
                  href="https://www.linkedin.com/in/sofia-sarhiri/"
                  className=""
                >
                  <Image
                    src="/linkedin.svg"
                    alt="linkedin Logo"
                    width={20}
                    height={20}
                    className="mr-4 inline-block"
                    priority
                  />
                </Link>
              </li>
            </ul>
            <p className="text-gray-500 text-sm sm:text-base pt-8">
              © Copyright 2024. All rights reserved.
            </p>
          </div>
        </div>
      </div>

      {/* Modal */}
      <Modal isOpen={isModalOpen} onClose={handleCloseModal}>
        {/* Modal content will be here */}
        <p></p>
      </Modal>
    </footer>
  );
};

export default Footer;
