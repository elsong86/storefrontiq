import React, { useRef, useState, FormEvent } from 'react';
import emailjs from '@emailjs/browser';

const Template: React.FC = () => {
  const form = useRef<HTMLFormElement>(null);
  const [successMessage, setSuccessMessage] = useState<string>('');

  const sendEmail = (e: FormEvent) => {
    e.preventDefault();

    if (form.current !== null) {

      emailjs.sendForm(
        process.env.NEXT_PUBLIC_SERVICE_ID!,
        process.env.NEXT_PUBLIC_TEMPLATE_ID!,
        form.current,
        {publicKey: process.env.NEXT_PUBLIC_PUBLIC_KEY!},
      )      
        .then(
          () => {
            console.log('SUCCESS!');
            setSuccessMessage('Message Sent Successfully!');
            // form.current.reset();
          },
          (error) => {
            console.log('FAILED...', error);
            window.alert('Uh-Oh, there was a problem. Please try again!');
          },
        );
    }
  };

  return (
    <div className="mx-auto max-w-screen-xl px-4 py-16 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-lg text-center">
        <h1 className="text-2xl font-bold sm:text-3xl">
          We Value Your <br />
          <span className="text-rose-800 pl-2">F</span>
          <span className="text-emerald-800">E</span>
          <span className="text-yellow-600">E</span>
          <span className="text-orange-700">D </span>
          <span className="text-rose-800">B</span>
          <span className="text-emerald-800">A</span>
          <span className="text-yellow-600">C</span>
          <span className="text-orange-700">K</span>
        </h1>
      </div>

      <form ref={form} onSubmit={sendEmail} className="mx-auto mb-0 mt-8 max-w-md space-y-4">
        <div>
          <label>Name</label>
          <div className="relative">
            <input
              type="text"
              name="user_name"
              className="w-full rounded-lg border-gray-200 p-4 pe-12 text-sm shadow-lg"
              placeholder="Your Name"
              required
            />
          </div>

        </div>

        <div>
          <label>Email</label>
          <div className="relative">
            <input
              type="text"
              name="user_email"
              className="w-full rounded-lg border-gray-200 p-4 pe-12 text-sm shadow-lg"
              placeholder="Enter Email"
              required
            />
          </div>
        </div>

        <div>
          <label>Message:</label>
          <div className="relative">
            <input
              type="text"
              name="message"
              className="w-full rounded-lg border-gray-200 p-4 pe-12 text-sm shadow-lg pb-20"
              placeholder="Leave your feedback:"
              required
            />
          </div>
        </div>

        <div className="flex items-center justify-between">
          <button
            type="submit"
            value="Send"
            className="inline-block rounded-lg bg-gradient-to-r from-rose-600 to-yellow-600 px-5 py-3 text-sm font-medium text-white hover:shadow-2xl"
          >
            Submit
          </button>
        </div>
      </form>

      {successMessage && (
        <div className="mt-4 text-center text-green-600">
          {successMessage}
        </div>
      )}

    </div>
  );
};

export default Template;
