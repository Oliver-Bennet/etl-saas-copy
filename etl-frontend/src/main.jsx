import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'


// BẮT BUỘC PHẢI CÓ 2 DÒNG NÀY – thiếu là đen thui!
import '@aws-amplify/ui-react/styles.css'

import { Amplify } from 'aws-amplify'


// THAY 4 THÔNG SỐ NÀY BẰNG CỦA BẠN
// Amplify.configure({
//   Auth: {
//     region: 'ap-southeast-1',                          // ← region của bạn
//     userPoolId: 'ap-southeast-1_LZU2SXqyz',            // ← User Pool ID
//     userPoolWebClientId: '6v29eis60gqjicijhirvp22of', // ← App Client ID (etl-users)
//     oauth: {
//       domain: 'ap-southeast-1lzu2sxqyz.auth.ap-southeast-1.amazoncognito.com', // ← Cognito domain
//       scope: ['email', 'openid', 'profile'],
//       redirectSignIn: 'http://localhost:5173/',         // để test local
//       redirectSignOut: 'http://localhost:5173/',
//       responseType: 'code'
//     }
//   }
// })


Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: 'ap-southeast-1_LZU2SXqyz',
      userPoolClientId: '6v29eis60gqjicijhirvp22of',
      loginWith: {
        email: true,
        username: false
      }
    }
  },
  oauth: {
    domain: 'ap-southeast-1lzu2sxqyz.auth.ap-southeast-1.amazoncognito.com',
    scope: ['email', 'openid', 'profile'],
    redirectSignIn: 'http://localhost:5173/',
    redirectSignOut: 'http://localhost:5173/',
    responseType: 'code'
  }
})

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)